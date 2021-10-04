import os

import dataflows as DF
from dataflows_elasticsearch import dump_to_es
from tableschema_elasticsearch.mappers import MappingGenerator

import elasticsearch

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


def data_api_es_flow():

    es_instance = elasticsearch.Elasticsearch(
        [dict(host=os.environ['ES_HOST'], port=int(os.environ['ES_PORT']))], timeout=60,
        **({"http_auth": os.environ['ES_HTTP_AUTH'].split(':')} if os.environ.get('ES_HTTP_AUTH') else {})
    )

    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
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
        DF.update_resource('card_data', name='cards'),
        dump_to_es(
            indexes=dict(srm__cards=[dict(resource_name='cards')]),
            mapper_cls=SRMMappingGenerator,
            engine=es_instance,
        ),
    )


def operator(*_):
    logger.info('Starting ES Flow')

    data_api_es_flow().process()

    logger.info('Finished ES Flow')


if __name__ == '__main__':
    operator(None, None, None)
