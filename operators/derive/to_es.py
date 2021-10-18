import os

import dataflows as DF
import elasticsearch
from dataflows_ckan import dump_to_ckan
from dataflows_elasticsearch import dump_to_es
from tableschema_elasticsearch.mappers import MappingGenerator

from conf import settings
from srm_tools.logger import logger


class SRMMappingGenerator(MappingGenerator):
    @classmethod
    def _convert_type(cls, schema_type, field, prefix):
        # TODO: should be in base class
        if field['type'] == 'any':
            field['es:itemType'] = 'string'
        prop = super()._convert_type(schema_type, field, prefix)
        boost, keyword = field.get('es:boost'), field.get('es:keyword')
        if keyword:
            prop['type'] = 'keyword'
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
        dump_to_es(
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
    return DF.Flow(
        DF.load(url, format='datapackage'),
        DF.update_package(title='Bounds for Locations in Israel', name='bounds-for-locations'),
        DF.update_resource(-1, name='places'),
        dump_to_es(
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


def operator(*_):
    logger.info('Starting ES Flow')
    data_api_es_flow().process()
    load_locations_to_es_flow().process()
    logger.info('Finished ES Flow')


if __name__ == '__main__':
    operator(None, None, None)
