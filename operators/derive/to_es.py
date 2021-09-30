import dataflows as DF
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


def data_api_es_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.set_type('card_id', **{'es:keyword': True}),
        DF.set_type('branch_id', type='string', **{'es:keyword': True}),
        DF.set_type('service_id', type='string', **{'es:keyword': True}),
        DF.set_type('organization_id', type='string', **{'es:keyword': True}),
        DF.set_type(
            'response_categories', type='array', **{'es:itemType': 'string', 'es:keyword': True}
        ),
        DF.set_type(
            'situations',
            type='array',
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
            type='array',
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
        dump_to_es(
            indexes=dict(cards=[dict(resource_name='card_data')]),
            mapper_cls=SRMMappingGenerator,
        ),
    )


def operator(*_):
    logger.info('Starting ES Flow')

    data_api_es_flow().process()

    logger.info('Finished ES Flow')


if __name__ == '__main__':
    operator(None, None, None)
