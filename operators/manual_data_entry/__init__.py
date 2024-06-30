import dataflows as DF

from dataflows_airtable import load_from_airtable

from conf import settings
from srm_tools.logger import logger
from srm_tools.stats import Stats
from .mde_utils import load_manual_data
from .external import main as load_external_data

stats = Stats()

def mde_prepare():
    source_flow = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.filter_rows(lambda r: r.get('Org Id') != 'dummy'),
        stats.filter_with_stats('Manual Data Entry: No Org ID or Org Name', lambda r: (r.get('Org Id') or r.get('Org Name'))),
        stats.filter_with_stats('Manual Data Entry: Entry not ready to publish', lambda r: r.get('Status') == 'בייצור'),
    )

    data_sources = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, 'DataReferences', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    ).results()[0][0]
    data_sources = dict((r['name'], r['reference']) for r in data_sources)
    return source_flow, data_sources


def operator(*_):
    logger.info('Starting Manual Data Entry Flow')
        
    source_flow, data_sources = mde_prepare()
    load_manual_data(source_flow, data_sources)

    logger.info('Finished Manual Data Entry Flow')

    logger.info('Starting External Manual Data Entry Flow')

    load_external_data()

    logger.info('Finished External Manual Data Entry Flow')


if __name__ == '__main__':
    operator(None, None, None)
