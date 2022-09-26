import dataflows as DF
from dataflows_ckan import dump_to_ckan

from conf import settings

from srm_tools.logger import logger

TEMPLATES = [
    '{response}', '{situation}', '{response} עבור {situation}'
]

def unwind_templates():
    def func(rows):
        for row in rows:
            # print(row)
            for template in TEMPLATES:
                responses = [(r['name'], r['synonyms']) for r in row['responses']] if '{response}' in template else [(None, None)]
                situations = [(s['name'], s['synonyms']) for s in row['situations']] if '{situation}' in template else [(None, None)]
                for response, rsyn in responses:
                    for situation, ssyn in situations:
                        yield {
                            'query': template.format(response=response, situation=situation),
                            'response': response,
                            'situation': situation,
                            'synonyms': rsyn + ssyn if rsyn and ssyn else rsyn or ssyn,
                        }
    return func


def autocomplete_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_resource(-1, name='autocomplete'),
        DF.add_field('query', 'string'),
        DF.add_field('response', 'string'),
        DF.add_field('situation', 'string'),
        DF.add_field('synonyms', 'array'),
        unwind_templates(),
        DF.join_with_self('autocomplete', ['query'], fields=dict(
            score=dict(aggregate='count'),
            query=None, response=None, situation=None, synonyms=None
        )),
        DF.set_type('query', **{'es:autocomplete': True, 'es:title': True}),
        DF.set_type('response', **{'es:keyword': True}),
        DF.set_type('situation', **{'es:keyword': True}),
        DF.set_type('synonyms', **{'es:itemType': 'string'}),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/autocomplete'),
    )

def operator(*_):
    logger.info('Starting AC Flow')
    autocomplete_flow().process()
    logger.info('Finished AC Flow')


if __name__ == '__main__':
    operator(None, None, None)
