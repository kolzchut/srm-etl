from . import to_dp, to_es, to_mapbox, to_sitemap, from_curation, autocomplete, to_sql

from srm_tools.logger import logger
from srm_tools.error_notifier import invoke_on



def deriveData(*_):

    logger.info('Starting Derive Data Flow')

    from_curation.operator()
    to_dp.operator()
    autocomplete.operator()
    to_es.operator()
    to_sql.operator()
    to_mapbox.operator()
    to_sitemap.operator()

    logger.info('Finished Derive Data Flow')


def operator(*_):
    invoke_on(deriveData, 'Upload to DB (Derive)')

if __name__ == '__main__':
    operator()
