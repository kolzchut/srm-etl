import math

import dataflows as DF
from dataflows_ckan import dump_to_ckan

from conf import settings

from srm_tools.logger import logger

TEMPLATES = [
    '{response}', '{situation}', '{response} עבור {situation}', '{org_name}', '{response} של {org_name}'
]

IGNORE_SITUATIONS = {
    'human-situations:languages:hebrew',
    'human-situations:age-groups:adults',
}

def unwind_templates():
    def func(rows):
        for row in rows:
            # print(row)
            for template in TEMPLATES:
                responses = [r for r in row['responses']] if '{response}' in template else [dict()]
                situations = [s for s in row['situations']] if '{situation}' in template else [dict()]
                org_names = row.get('organization_short_name') or row.get('organization_name')
                if org_names:
                    org_names = [org_names]
                else:
                    org_names = [] if '{org_name}' in template else [None]
                for response in responses:
                    for situation in situations:
                        for org_name in org_names:
                            if situation.get('id') in IGNORE_SITUATIONS:
                                continue
                            query = template.format(response=response.get('name'), situation=situation.get('name'), org_name=org_name)
                            yield {
                                'query': query,
                                'query_heb': query,
                                'response': response.get('id'),
                                'situation': situation.get('id'),
                                'org_id': row['organization_id'],
                                'org_name': org_name,
                                'synonyms': response.get('synonyms', []) + situation.get('synonyms', [])
                            }


    return func


def autocomplete_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_resource(-1, name='autocomplete'),
        DF.add_field('query', 'string'),
        DF.add_field('query_heb', 'string'),
        DF.add_field('response', 'string'),
        DF.add_field('situation', 'string'),
        DF.add_field('synonyms', 'array'),
        DF.add_field('org_id', 'string'),
        DF.add_field('org_name', 'string'),
        unwind_templates(),
        DF.join_with_self('autocomplete', ['query'], fields=dict(
            score=dict(aggregate='count'),
            query=None, query_heb=None, response=None, situation=None, synonyms=None, org_id=None, org_name=None
        )),
        DF.set_type('score', type='number', transform=lambda v: (math.log(v) + 1)**2),
        DF.set_type('query', **{'es:autocomplete': True, 'es:title': True}),
        DF.set_type('query_heb', **{'es:title': True}),
        DF.set_type('response', **{'es:keyword': True}),
        DF.set_type('situation', **{'es:keyword': True}),
        DF.set_type('org_id', **{'es:keyword': True}),
        DF.set_type('synonyms', **{'es:itemType': 'string'}),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/autocomplete'),
    )

def operator(*_):
    logger.info('Starting AC Flow')
    autocomplete_flow().process()
    logger.info('Finished AC Flow')


if __name__ == '__main__':
    operator(None, None, None)
