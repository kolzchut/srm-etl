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
        ).results(on_error=None)[0][0]
        keys = [n for rec in all_places for n in rec['name']]
        mapping = dict((n ,rec['bounds']) for rec in all_places for n in rec['name'])
        return keys, mapping

def remove_stop_words(s):
    return ' '.join([w for w in s.split() if w not in STOP_WORDS])

def unwind_templates():
    def func(rows):
        for row in rows:
            # print(row)
            for importance, template in enumerate(TEMPLATES):
                responses = row['responses_parents'] if '{response}' in template else [dict()]
                situations = row['situations_parents'] if '{situation}' in template else [dict()]
                direct_responses = row['response_ids'] + [None]
                direct_situations = row['situation_ids'] + [None]
                if '{org_name}' in template or '{org_id}' in template:
                    org_name = row.get('organization_short_name') or row.get('organization_name')
                    _org_names = [row.get('branch_operating_unit'), row.get('organization_short_name'), row.get('organization_name'), row.get('organization_original_name')]
                    org_names = []
                    for on in _org_names:
                        if on and on not in org_names:
                            org_names.append(on)
                else:
                    org_name = None
                    org_names = [None]
                org_id = row.get('organization_id') if org_name else None
                org_ids = [org_id]
                city_name = row.get('branch_city') if '{city_name}' in template else None
                city_names = [city_name]
                visible = '{org_id}' not in template
                for response, situation, org_name, org_id, city_name in product(responses, situations, org_names, org_ids, city_names):
                    situation_id = situation.get('id')
                    response_id = response.get('id')
                    if situation_id in IGNORE_SITUATIONS:
                        continue
                    if org_id and VERIFY_ORG_ID.match(org_id) is None:
                        continue
                    if city_name and VERIFY_CITY_NAME.match(city_name) is None:
                        continue
                    if situation_id is not None and len(situation_id.split(':')) < 3 and situation_id not in (
                        'human_situations:armed_forces', 'human_situations:survivors', 'human_situations:substance_dependency',
                        'human_situations:health', 'human_situations:disability', 'human_situations:criminal_history',
                        'human_situations:mental_health',
                    ):
                        continue
                    low = False
                    if situation_id not in direct_situations:
                        low = True
                    if response_id not in direct_responses:
                        low = True
                    if org_name and row['organization_branch_count'] < 5:
                        low = True
                    query = template.format(response=response.get('name'), situation=situation.get('name'), org_name=org_name, org_id=org_id, city_name=city_name)
                    if 'None' in query:
                        continue
                    query_heb = template.format(response=response.get('name'), situation=situation.get('name'), org_name=org_name, org_id=org_name, city_name=city_name)
                    structured_query = set([
                        response.get('name'),
                        situation.get('name'),
                        city_name,
                        *response.get('synonyms', []),
                        *situation.get('synonyms', []),
                        org_name,
                    ])
                    structured_query = ' '.join(remove_stop_words(x.strip()) for x in structured_query if x)
                    yield {
                        'query': query,
                        'query_heb': query_heb,
                        'response': response_id,
                        'situation': situation_id,
                        'org_id': org_id,
                        'org_name': org_name,
                        'city_name': city_name,
                        'synonyms': response.get('synonyms', []) + situation.get('synonyms', []),
                        'response_name': response.get('name'),
                        'situation_name': situation.get('name'),
                        'structured_query': structured_query,
                        'visible': visible,
                        'low': low,
                        'importance': importance,
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
        DF.add_field('low', 'boolean'),
        DF.add_field('importance', 'integer'),
        unwind_templates(),
        DF.sort_rows(['importance']),
        DF.join_with_self('autocomplete', ['query'], fields=dict(
            score=dict(aggregate='count'),
            query=None, query_heb=dict(aggregate='first'), importance=dict(aggregate='first'),
            response=dict(aggregate='first'), situation=dict(aggregate='first'), synonyms=dict(aggregate='first'), 
            org_id=dict(aggregate='first'), org_name=dict(aggregate='first'), city_name=dict(aggregate='first'),
            response_name=dict(aggregate='first'), situation_name=dict(aggregate='first'), structured_query=dict(aggregate='first'), 
            visible=dict(aggregate='first'), low=dict(aggregate='min'),
        )),
        DF.add_field('bounds', 'array', **{'es:itemType': 'number', 'es:index': False}),
        get_bounds(),
        DF.set_type('score', type='number', transform=lambda v: (math.log(v) + 1)**2),
        DF.set_type('score', transform=lambda v, row: 0.5 if row['low'] else v),
        DF.set_type('query', **{'es:autocomplete': True, 'es:hebrew': True}),
        DF.set_type('query_heb', **{'es:hebrew': True}),
        DF.set_type('structured_query', **{'es:hebrew': True}),
        DF.set_type('response', **{'es:keyword': True}),
        DF.set_type('situation', **{'es:keyword': True}),
        DF.set_type('org_id', **{'es:keyword': True}),
        DF.set_type('synonyms', **{'es:itemType': 'string', 'es:hebrew': True}),
        DF.add_field('id', 'string', lambda r: '_'.join(PKRE.findall(r['query']))),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/autocomplete'),
    )

def operator(*_):
    logger.info('Starting AC Flow')
    autocomplete_flow().process()
    logger.info('Finished AC Flow')


if __name__ == '__main__':
    operator(None, None, None)
