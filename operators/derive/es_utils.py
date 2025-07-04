import uuid
import time

import dataflows as DF

import elasticsearch
from dataflows_elasticsearch import dump_to_es
from tableschema_elasticsearch.mappers import MappingGenerator

from conf import settings


def es_instance():
    return elasticsearch.Elasticsearch(
        [dict(host=settings.ES_HOST, port=int(settings.ES_PORT))],
        timeout=60, retry_on_timeout=True, max_retries=3, retry_on_status=[502, 503, 504],
        **({"http_auth": settings.ES_HTTP_AUTH.split(':')} if settings.ES_HTTP_AUTH else {}),
    )


class SRMMappingGenerator(MappingGenerator):
    @classmethod
    def _convert_type(cls, schema_type, field, prefix):
        # TODO: should be in base class
        if field['type'] == 'any':
            field['es:itemType'] = 'string'
        prop = super()._convert_type(schema_type, field, prefix)
        keyword, autocomplete, hebrew = field.get('es:keyword'), field.get('es:autocomplete'), field.get('es:hebrew')
        if keyword:
            prop['type'] = 'keyword'
        if autocomplete:
            prop['type'] = 'search_as_you_type'
        if schema_type in ('number', 'integer', 'geopoint'):
            prop['index'] = True
        if hebrew or field['name'].split('_')[-1] in ('name', 'purpose', 'description', 'details', 'synonyms', 'heb'):
            prop['fields'] = {
                'hebrew': {
                    'type': 'text',
                    'analyzer': 'hebrew'
                }
            }

        return prop


def dump_to_es_and_delete(**kwargs):
    unique_id = uuid.uuid4().hex
    engine: elasticsearch.Elasticsearch = es_instance()
    try:
        success = engine.ping()
        assert success
    except:
        print('FAILED TO CONNECT TO ES')
        return
    indexes = list(kwargs.get('indexes').keys())
    kwargs.setdefault('engine', engine)
    kwargs.setdefault('mapper_cls', SRMMappingGenerator)

    def deleter():
        time.sleep(30) # wait for ES to settle
        for index in indexes:
            response = engine.delete_by_query(index, body=dict(query=dict(bool=dict(must_not=dict(term=dict(revision=unique_id))))), conflicts='proceed')
            print('DELETED', index, response)

    return DF.Flow(
        DF.add_field('revision', 'string', unique_id, **{'es:keyword': True}),
        dump_to_es(**kwargs),
        DF.delete_fields(['revision']),
        DF.finalizer(deleter)
    )
