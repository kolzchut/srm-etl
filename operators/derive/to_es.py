import tempfile
import shutil
from dataflows_airtable.load_from_airtable import load_from_airtable
import requests

import dataflows as DF
from dataflows_ckan import dump_to_ckan
import re

from conf import settings

from . import helpers
from .es_utils import dump_to_es_and_delete

from srm_tools.logger import logger
from srm_tools.unwind import unwind

def card_score(row):
    branch_count = row['organization_branch_count'] or 1
    national_service = bool(row['national_service'])
    if national_service:
        score = 10
        phone_numbers = list(filter(None, (row['service_phone_numbers'] or []) + (row['organization_phone_numbers'] or [])))
        if phone_numbers:
            phone_number = phone_numbers[0]
            if len(phone_numbers) <= 5 or phone_number.startswith('1'):
                score = 50
    else:
        score = 1 + branch_count / 10
    response_ids = row['response_ids'] or []
    if 'human_services:internal_emergency_services' in response_ids:
        score *= 10
    organization_kind = row['organization_kind']
    if organization_kind in ('משרד ממשלתי', 'רשות מקומית'):
        score *= 5

    return score

def data_api_es_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_package(title='Card Data', name='srm_card_data'),
        DF.update_resource('card_data', name='cards'),
        DF.add_field('score', 'number', card_score, resources=['cards']),
        DF.set_type(
            'situations',
            **{
                'es:itemType': 'object',
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'id', 'es:keyword': True},
                        {'type': 'string', 'name': 'name', 'es:title': True},
                        {'type': 'string', 'name': 'synonyms', 'es:title': True},
                    ]
                },
            },
        ),
        DF.set_type(
            'responses',
            **{
                'es:itemType': 'object',
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'id', 'es:keyword': True},
                        {'type': 'string', 'name': 'name', 'es:title': True},
                        {'type': 'string', 'name': 'synonyms', 'es:title': True},
                    ]
                },
            },
        ),
        DF.set_type(
            'situations_parents',
            **{
                'es:itemType': 'object',
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'id', 'es:keyword': True},
                        {'type': 'string', 'name': 'name', 'es:title': True},
                        {'type': 'string', 'name': 'synonyms', 'es:title': True},
                    ]
                },
            },
        ),
        DF.set_type(
            'responses_parents',
            **{
                'es:itemType': 'object',
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'id', 'es:keyword': True},
                        {'type': 'string', 'name': 'name', 'es:title': True},
                        {'type': 'string', 'name': 'synonyms', 'es:title': True},
                    ]
                },
            },
        ),
        DF.set_type(
            'service_urls',
            **{
                'es:itemType': 'object',
                'es:index': False,
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'href'},
                        {'type': 'string', 'name': 'text'},
                    ]
                },
            },
        ),
        DF.set_type(
            'branch_urls',
            **{
                'es:itemType': 'object',
                'es:index': False,
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'href'},
                        {'type': 'string', 'name': 'text'},
                    ]
                },
            },
        ),
        DF.set_type(
            'organization_urls',
            **{
                'es:itemType': 'object',
                'es:index': False,
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'href'},
                        {'type': 'string', 'name': 'text'},
                    ]
                },
            },
        ),
        DF.set_type(
            'branch_email_address',
            **{
                'es:itemType': 'string',
                'es:index': False,
            },
        ),
        DF.set_type(
            'organization_phone_numbers',
            **{
                'es:itemType': 'string',
                'es:index': False,
            },
        ),
        DF.set_type(
            'organization_email_address',
            **{
                'es:itemType': 'string',
                'es:index': False,
            },
        ),
        DF.set_type(
            'branch_phone_numbers',
            **{
                'es:itemType': 'string',
                'es:index': False,
            },
        ),
        DF.set_type(
            'service_phone_numbers',
            **{
                'es:itemType': 'string',
                'es:index': False,
            },
        ),
        DF.set_type(
            'service_email_address',
            **{
                'es:itemType': 'string',
                'es:index': False,
            },
        ),
        DF.set_type(
            'data_sources',
            **{
                'es:itemType': 'string',
                'es:index': False,
            },
        ),
        DF.set_type(
            'response_ids',
            **{
                'es:itemType': 'string',
                'es:keyword': True,
            },
        ),
        DF.set_type(
            'situation_ids',
            **{
                'es:itemType': 'string',
                'es:keyword': True,
            },
        ),
        dump_to_es_and_delete(
            indexes=dict(srm__cards=[dict(resource_name='cards')]),
        ),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
        ),

        # # TESTING FLOW
        # DF.add_field('text', 'array', **{'es:itemType': 'string', 'es:keyword': True}, default=select_text_fields),
        # DF.select_fields(['card_id', 'text']),
        # dump_to_es_and_delete(
        #     indexes=dict(testing=[dict(resource_name='cards')]),
        # ),
    )

HEB = re.compile('[א-ת]+[-א-ת"״]+[א-ת]+')
def select_text_fields(row):
    def _aux(obj):
        if not obj:
            pass
        elif isinstance(obj, dict):
            for k, v in obj.items():
                if k not in ('data_sources', 'service_urls', 'branch_urls', 'organization_urls', 'possible_autocomplete'):
                    yield from _aux(v)
        elif isinstance(obj, list):
            for v in obj:
                yield from _aux(v)
        elif isinstance(obj, str):
            yield from HEB.findall(obj)
    return list(_aux(row))

def load_locations_to_es_flow():
    url = settings.LOCATION_BOUNDS_SOURCE_URL
    scores = dict(
        city=100, town=50, village=10, hamlet=5,
    )
    def calc_score(r):
        b = r['bounds']
        size = (b[2] - b[0]) * (b[3] - b[1]) * 100000
        return size * scores.get(r['place'], 1)

    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmpfile:
        src = requests.get(url, stream=True).raw
        shutil.copyfileobj(src, tmpfile)
        tmpfile.close()
        return DF.Flow(
            DF.load(tmpfile.name, format='datapackage'),
            DF.update_package(title='Bounds for Locations in Israel', name='bounds-for-locations'),
            DF.update_resource(-1, name='places'),
            # DF.set_type('name', **{'es:autocomplete': True}),
            DF.add_field('query', 'string', lambda r: sorted(r['name'], key=lambda v: len(v), reverse=True)[0]),
            DF.add_field('score', 'number', calc_score),
            DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/place_data'),
            dump_to_es_and_delete(
                indexes=dict(srm__places=[dict(resource_name='places')]),
            ),
            dump_to_ckan(
                settings.CKAN_HOST,
                settings.CKAN_API_KEY,
                settings.CKAN_OWNER_ORG,
            ),
        )

def load_responses_to_es_flow():
    
    def print_top(row):
        parts = row['id'].split(':')
        if len(parts) == 2:
            print('STATS', parts[1], row['count'])

    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.add_field('response_ids', 'array', lambda r: [r['id'] for r in r['responses']]),
        DF.set_type('response_ids', transform=lambda v: helpers.update_taxonomy_with_parents(v)),
        DF.select_fields(['response_ids']),
        unwind('response_ids', 'id', 'object'),
        DF.join_with_self('card_data', ['id'], dict(
            id=None,
            count=dict(aggregate='count')
        )),
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_RESPONSE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_package(title='Taxonomy Responses', name='responses'),
        DF.update_resource(-1, name='responses'),
        DF.join('card_data', ['id'], 'responses', ['id'], dict(
            count=None
        )),
        DF.filter_rows(lambda r: r.get('status') == 'ACTIVE'),
        DF.filter_rows(lambda r: r['count'] is not None),
        DF.select_fields(['id', 'name', 'synonyms', 'breadcrumbs', 'count']),
        DF.set_type('id', **{'es:keyword': True}),
        # DF.set_type('name', **{'es:autocomplete': True}),
        DF.set_type('synonyms', **{'es:itemType': 'string'}),
        DF.add_field('score', 'number', lambda r: r['count']),
        DF.set_primary_key(['id']),
        print_top,
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/response_data'),
        dump_to_es_and_delete(
            indexes=dict(srm__responses=[dict(resource_name='responses')]),
        ),
        DF.update_resource(-1, name='responses', path='responses.json'),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
            format='json'
        ),
        # DF.printer()
    )

def load_situations_to_es_flow():
    
    def print_top(row):
        parts = row['id'].split(':')
        if len(parts) == 2:
            print('STATS', parts[1], row['count'])

    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.add_field('situation_ids', 'array', lambda r: [r['id'] for r in r['situations']]),
        DF.set_type('situation_ids', transform=lambda v: helpers.update_taxonomy_with_parents(v)),
        DF.select_fields(['situation_ids']),
        unwind('situation_ids', 'id', 'object'),
        DF.join_with_self('card_data', ['id'], dict(
            id=None,
            count=dict(aggregate='count')
        )),
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_SITUATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_package(title='Taxonomy Situations', name='situations'),
        DF.update_resource(-1, name='situations'),
        DF.join('card_data', ['id'], 'situations', ['id'], dict(
            count=None
        )),
        DF.filter_rows(lambda r: r.get('status') == 'ACTIVE'),
        DF.filter_rows(lambda r: r['count'] is not None),
        DF.select_fields(['id', 'name', 'synonyms', 'breadcrumbs', 'count']),
        DF.set_type('id', **{'es:keyword': True}),
        DF.set_type('synonyms', **{'es:itemType': 'string'}),
        DF.add_field('score', 'number', lambda r: r['count']),
        DF.set_primary_key(['id']),
        print_top,
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/situation_data'),
        dump_to_es_and_delete(
            indexes=dict(srm__situations=[dict(resource_name='situations')]),
        ),
        DF.update_resource(-1, name='situations', path='situations.json'),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
            format='json'
        ),
        # DF.printer()
    )

def load_organizations_to_es_flow():
    return DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json', resources=['organizations'],
        ),
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.join_with_self('card_data', ['organization_id'], dict(
            id=dict(name='organization_id'),
            count=dict(aggregate='count')
        )),
        DF.join(
            'organizations', ['id'], 'card_data', ['id'],
            dict(name=None, description=None, kind=None)
        ),
        DF.sort_rows('{count}'),
        DF.update_package(title='Active Organizations', name='organizations'),
        DF.update_resource(-1, name='orgs'),
        # DF.select_fields(['id', 'name', 'description', 'kind']),
        DF.set_type('id', **{'es:keyword': True}),
        # DF.set_type('name', **{'es:autocomplete': True}),
        DF.set_type('description'),
        DF.set_type('kind', **{'es:keyword': True}),
        DF.add_field('score', 'number', lambda r: 10*r['count']),
        DF.set_primary_key(['id']),
        dump_to_es_and_delete(
            indexes=dict(srm__orgs=[dict(resource_name='orgs')]),
        ),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
        ),
    )

def load_autocomplete_to_es_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/autocomplete/datapackage.json'),
        DF.update_package(title='AutoComplete Queries', name='autocomplete'),
        DF.set_primary_key(['id']),
        dump_to_es_and_delete(
            indexes=dict(srm__autocomplete=[dict(resource_name='autocomplete')]),
        ),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
        ),
    )

def operator(*_):
    logger.info('Starting ES Flow')
    data_api_es_flow().process()
    load_locations_to_es_flow().process()
    load_responses_to_es_flow().process()
    load_situations_to_es_flow().process()
    load_organizations_to_es_flow().process()
    load_autocomplete_to_es_flow().process()
    logger.info('Finished ES Flow')


if __name__ == '__main__':
    operator(None, None, None)
