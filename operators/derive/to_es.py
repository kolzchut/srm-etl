import tempfile
import shutil
from dataflows_airtable.load_from_airtable import load_from_airtable
import requests
import uuid

import dataflows as DF
import elasticsearch
from dataflows_ckan import dump_to_ckan
from dataflows_elasticsearch import dump_to_es
from tableschema_elasticsearch.mappers import MappingGenerator

from conf import settings
from srm_tools.logger import logger

from . import helpers


def dump_to_es_and_delete(**kwargs):
    unique_id = uuid.uuid4().hex
    engine: elasticsearch.Elasticsearch = kwargs.get('engine')
    indexes = list(kwargs.get('indexes').keys())

    def deleter():
        for index in indexes:
            response = engine.delete_by_query(index, body=dict(query=dict(bool=dict(must_not=dict(term=dict(revision=unique_id))))))
            print('DELETED', index, response)

    return DF.Flow(
        DF.add_field('revision', 'string', unique_id, **{'es:keyword': True}),
        dump_to_es(**kwargs),
        DF.delete_fields(['revision']),
        DF.finalizer(deleter)
    )


class SRMMappingGenerator(MappingGenerator):
    @classmethod
    def _convert_type(cls, schema_type, field, prefix):
        # TODO: should be in base class
        if field['type'] == 'any':
            field['es:itemType'] = 'string'
        prop = super()._convert_type(schema_type, field, prefix)
        boost, keyword, autocomplete = field.get('es:boost'), field.get('es:keyword'), field.get('es:autocomplete')
        if keyword:
            prop['type'] = 'keyword'
        if autocomplete:
            prop['index_prefixes'] = {}
        if boost:
            prop['boost'] = boost
        if schema_type in ('number', 'integer', 'geopoint'):
            prop['index'] = True
        return prop


def es_instance():
    return elasticsearch.Elasticsearch(
        [dict(host=settings.ES_HOST, port=int(settings.ES_PORT))],
        timeout=60,
        **({"http_auth": settings.ES_HTTP_AUTH.split(':')} if settings.ES_HTTP_AUTH else {}),
    )

def data_api_es_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_package(title='Card Data', name='srm_card_data'),
        DF.update_resource('card_data', name='cards'),
        DF.add_field('score', 'number', 1),
        DF.set_type('card_id', **{'es:keyword': True}),
        DF.set_type('branch_id', **{'es:keyword': True}),
        DF.set_type('service_id', **{'es:keyword': True}),
        DF.set_type('organization_id', **{'es:keyword': True}),
        DF.set_type('response_categories', **{'es:itemType': 'string', 'es:keyword': True}),
        DF.set_type(
            'situations',
            **{
                'es:itemType': 'object',
                'es:schema': {
                    'fields': [
                        {'type': 'string', 'name': 'id', 'es:keyword': True},
                        {'type': 'string', 'name': 'name'},
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
                        {'type': 'string', 'name': 'name'},
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
            'branch_email_addresses',
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
        dump_to_es_and_delete(
            indexes=dict(srm__cards=[dict(resource_name='cards')]),
            mapper_cls=SRMMappingGenerator,
            engine=es_instance(),
        ),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
        ),
    )

def load_locations_to_es_flow():
    url = settings.LOCATION_BOUNDS_SOURCE_URL
    scores = dict(
        city=100, town=50, village=10, hamlet=5,
    )
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmpfile:
        src = requests.get(url, stream=True).raw
        shutil.copyfileobj(src, tmpfile)
        tmpfile.close()
        return DF.Flow(
            DF.load(tmpfile.name, format='datapackage'),
            DF.update_package(title='Bounds for Locations in Israel', name='bounds-for-locations'),
            DF.update_resource(-1, name='places'),
            DF.set_type('name', **{'es:autocomplete': True}),
            DF.add_field('score', 'number', lambda r: scores.get(r['place'], 1)),
            dump_to_es_and_delete(
                indexes=dict(srm__places=[dict(resource_name='places')]),
                mapper_cls=SRMMappingGenerator,
                engine=es_instance(),
            ),
            dump_to_ckan(
                settings.CKAN_HOST,
                settings.CKAN_API_KEY,
                settings.CKAN_OWNER_ORG,
            ),
        )

def load_responses_to_es_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.select_fields(['responses']),
        helpers.unwind('responses', 'response', 'object'),
        DF.add_field('id', 'string', lambda r: r['response']['id']),
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
        DF.filter_rows(lambda r: r['status'] == 'ACTIVE'),
        DF.filter_rows(lambda r: r['count'] is not None),
        DF.select_fields(['id', 'name', 'breadcrumbs', 'count']),
        DF.set_type('id', **{'es:keyword': True}),
        DF.set_type('name', **{'es:autocomplete': True}),
        DF.add_field('score', 'number', lambda r: 10*(6 - len(r['id'].split(':')))),
        DF.set_primary_key(['id']),
        dump_to_es_and_delete(
            indexes=dict(srm__responses=[dict(resource_name='responses')]),
            mapper_cls=SRMMappingGenerator,
            engine=es_instance(),
        ),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
        ),
        # DF.printer()
    )

def operator(*_):
    logger.info('Starting ES Flow')
    data_api_es_flow().process()
    load_locations_to_es_flow().process()
    load_responses_to_es_flow().process()
    logger.info('Finished ES Flow')


if __name__ == '__main__':
    operator(None, None, None)
