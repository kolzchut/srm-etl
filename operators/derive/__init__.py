from . import to_dp, to_es, to_mapbox, to_sitemap, from_curation, autocomplete, to_sql

from srm_tools.logger import logger
from srm_tools.error_notifier import invoke_on



def deriveData(*_):

    logger.info('Starting Derive Data Flow')

    from_curation.operator()
    logger.debug('---from_curation operator finished---')
    to_dp.operator()
    logger.debug('---to_dp operator finished---')
    autocomplete.operator()
    logger.debug('---autocomplete operator finished---')
    to_es.operator()
    logger.debug('---to_es operator finished---')
    to_sql.operator()
    logger.debug('---to_sql operator finished---')
    to_mapbox.operator()
    logger.debug('---to_mapbox operator finished---')
    to_sitemap.operator()

    logger.info('Finished Derive Data Flow')


def operator(*_):
    invoke_on(deriveData, 'Upload to DB (Derive)')

if __name__ == '__main__':
    operator()
