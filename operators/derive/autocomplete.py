import math
import re
import tempfile
import requests
import shutil
from itertools import product

from thefuzz import process

import dataflows as DF
from dataflows_ckan import dump_to_ckan

from conf import settings

from srm_tools.logger import logger

TEMPLATES = [
    '{response}',
    '{situation}',
    '{response} עבור {situation}',
    '{org_name}',
    '{response} של {org_name}',
    '{org_id}',
    '{response} ב{city_name}',
    'שירותים עבור {situation} ב{city_name}',
    '{response} עבור {situation} ב{city_name}',
    '{response} של {org_name} ב{city_name}',
]
STOP_WORDS = [
    'עבור',
    'של',
    'באיזור',
]

IGNORE_SITUATIONS = {
    'human_situations:language:hebrew_speaking',
    'human_situations:age_group:adults',
}

PKRE = re.compile('[0-9a-zA-Zא-ת]+')
VERIFY_ORG_ID = re.compile('^(srm|)[0-9]+$')
VERIFY_CITY_NAME = re.compile('''^[א-ת-`"' ]+$''')

def prepare_locations():
    url = settings.LOCATION_BOUNDS_SOURCE_URL
    all_places = []
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmpfile:
        src = requests.get(url, stream=True).raw
        shutil.copyfileobj(src, tmpfile)
        tmpfile.close()
        all_places = DF.Flow(
            DF.load(tmpfile.name, format='datapackage'),
        ).results()[0][0]
        keys = [n for rec in all_places for n in rec['name']]
        mapping = dict((n ,rec['bounds']) for rec in all_places for n in rec['name'])
        return keys, mapping

def remove_stop_words(s):
    return ' '.join([w for w in s.split() if w not in STOP_WORDS])

def unwind_templates():
    def func(rows):
        for row in rows:
            # print(row)
            for template in TEMPLATES:
                responses = [r for r in row['responses']] if '{response}' in template else [dict()]
                situations = [s for s in row['situations']] if '{situation}' in template else [dict()]
                org_name = row.get('organization_short_name') or row.get('organization_name') if '{org_name}' in template or '{org_id}' in template else None
                org_names = [org_name]
                org_id = row.get('organization_id') if org_name else None
                org_ids = [org_id]
                city_name = row.get('branch_city') if '{city_name}' in template else None
                city_names = [city_name]
                visible = '{org_id}' not in template
                for response, situation, org_name, org_id, city_name in product(responses, situations, org_names, org_ids, city_names):
                    if situation.get('id') in IGNORE_SITUATIONS:
                        continue
                    if org_id and VERIFY_ORG_ID.match(org_id) is None:
                        continue
                    if city_name and VERIFY_CITY_NAME.match(city_name) is None:
                        continue
                    query = template.format(response=response.get('name'), situation=situation.get('name'), org_name=org_name, org_id=org_id, city_name=city_name)
                    if 'None' in query:
                        continue
                    query_heb = template.format(response=response.get('name'), situation=situation.get('name'), org_name=org_name, org_id=org_name, city_name=city_name)
                    structured_query = set([
                        response.get('name'),
                        situation.get('name'),
                        city_name,
                        *response.get('synonyms', []),
                        *situation.get('synonyms', [])
                    ])
                    structured_query = ' '.join(remove_stop_words(x.strip()) for x in structured_query if x)
                    yield {
                        'query': query,
                        'query_heb': query_heb,
                        'response': response.get('id'),
                        'situation': situation.get('id'),
                        'org_id': org_id,
                        'org_name': org_name,
                        'city_name': city_name,
                        'synonyms': response.get('synonyms', []) + situation.get('synonyms', []),
                        'response_name': response.get('name'),
                        'situation_name': situation.get('name'),
                        'structured_query': structured_query,
                        'visible': visible,
                    }


    return func


def get_bounds():
    location_keys, location_mapping = prepare_locations()
    cache = dict()
    def func(rows):
        for row in rows:
            city_name = row['city_name']
            if city_name:
                if city_name in cache:
                    row['bounds'] = cache[city_name]
                    yield row
                    continue
                match = process.extractOne(city_name, location_keys, score_cutoff=80)
                if match:
                    cache[city_name] = location_mapping[match[0]]
                    row['bounds'] = cache[city_name]
                    yield row
                else:
                    cache[city_name] = None
                    print('UNKNOWN CITY', city_name)
            else:
                yield row
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
        DF.add_field('city_name', 'string'),
        DF.add_field('response_name', 'string'),
        DF.add_field('situation_name', 'string'),
        DF.add_field('structured_query', 'string'),
        DF.add_field('visible', 'boolean'),
        unwind_templates(),
        DF.join_with_self('autocomplete', ['query'], fields=dict(
            score=dict(aggregate='count'),
            query=None, query_heb=None, response=None, situation=None, synonyms=None, 
            org_id=None, org_name=None, city_name=None,
            response_name=None, situation_name=None, structured_query=None, visible=None
        )),
        DF.add_field('bounds', 'array', **{'es:itemType': 'number', 'es:index': False}),
        get_bounds(),
        DF.set_type('score', type='number', transform=lambda v: (math.log(v) + 1)**2),
        DF.set_type('query', **{'es:autocomplete': True, 'es:title': True}),
        DF.set_type('query_heb', **{'es:title': True}),
        DF.set_type('structured_query', **{'es:hebrew': True}),
        DF.set_type('response', **{'es:keyword': True}),
        DF.set_type('situation', **{'es:keyword': True}),
        DF.set_type('org_id', **{'es:keyword': True}),
        DF.set_type('synonyms', **{'es:itemType': 'string', 'es:title': True}),
        DF.add_field('id', 'string', lambda r: '_'.join(PKRE.findall(r['query']))),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/autocomplete'),
    )

def operator(*_):
    logger.info('Starting AC Flow')
    autocomplete_flow().process()
    logger.info('Finished AC Flow')


if __name__ == '__main__':
    operator(None, None, None)
