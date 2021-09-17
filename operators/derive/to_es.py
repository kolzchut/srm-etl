import dataflows as DF
from dataflows.helpers.resource_matcher import ResourceMatcher
from dataflows_elasticsearch import dump_to_es

from conf import settings
from srm_tools.logger import logger


def set_es_item_types(resources=None):
    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                for field in resource["schema"]["fields"]:
                    if field["type"] == "any":
                        field["type"] = "string"
                    elif field["type"] == "array":
                        field["es:itemType"] = "string"
        yield package.pkg

        res_iter = iter(package)
        for r in res_iter:
            if matcher.match(r.res.name):
                yield r.it
            else:
                yield r

    return func


def data_api_es_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/table_data/datapackage.json'),
        DF.printer(),
        # the tableschema-elasticsearch driver requires
        # a primary key but elasticsearch does not.
        DF.set_primary_key(
            [
                'response_id',
                'situation_id',
                'service_id',
                'organization_id',
                'branch_id',
            ]
        ),
        dump_to_es(
            indexes=dict(
                # TODO - looks like we should default doc_type inside the dumper, and make it optional in dump_to_es
                # https://github.com/elastic/elasticsearch-py/issues/846
                # TODO - warnings
                # ElasticsearchWarning: [types removal] Using include_type_name in put mapping requests is deprecated.
                # ElasticsearchWarning: [types removal] Specifying types in bulk requests is deprecated.
                srm_apiz=[dict(doc_type="_doc", resource_name='table_data')]
            ),
        ),
    )


def operator(*_):
    logger.info('Starting ES Flow')

    data_api_es_flow().process()

    logger.info('Finished ES Flow')


if __name__ == '__main__':
    operator(None, None, None)
