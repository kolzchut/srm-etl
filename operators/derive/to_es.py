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
        DF.load(f'{settings.DATA_DUMP_DIR}/flat_table/datapackage.json'),
        dump_to_es(
            indexes=dict(srm_api=[dict(resource_name='flat_table')]),
        ),
    )


def operator(*_):
    logger.info('Starting ES Flow')

    data_api_es_flow().process()

    logger.info('Finished ES Flow')


if __name__ == '__main__':
    operator(None, None, None)
