import datetime
from srm_tools.budgetkey import fetch_from_budgetkey
from copy import deepcopy

from srm_tools.logger import logger
from srm_tools.processors import update_mapper
from srm_tools.update_table import airtable_updater
from srm_tools.situations import Situations
from srm_tools.error_notifier import invoke_on

from conf import settings
from operators.soproc.click_scraper import scrape_click
from srm_tools.error_notifier import send_failure_email
situations = Situations()


def verifyDataExistance(service, counter, max_count = 10):
    if not service['name'] and not service['description']:
        logger.error('SoProc: Service has no name or description')
        send_failure_email('SoProc', 'Service name is invalid')
    elif not service['name']:
        logger.error('SoProc: Service name is empty')
        send_failure_email('SoProc', 'Service name is empty')
    elif not service['description']:
        logger.error('SoProc: Service description is empty')
        send_failure_email('SoProc', 'Service description is empty')
    if not service['name'] or not service['description']:
        counter += 1
        if counter > max_count:
            send_failure_email('SoProc', 'Too many services with missing data')
            logger.error('SoProc: Too many services with missing data')
            raise Exception('Too many services with missing data')

## ORGANIZATIONS
def updateFromSourceData():
    def func(rows):
        for row in rows:
            data = row.get('data')
            if data is not None:
                row.update(data)
            yield row
    return func

def fetchOrgData():
    print('FETCHING ALL ORGANIZATIONS')
    today = datetime.date.today().isoformat()
    query = '''
        with suppliers as (select jsonb_array_elements(suppliers) as supplier from activities where suppliers is not null and suppliers::text != 'null')
        select supplier->>'entity_id' as entity_id, supplier->>'entity_kind' as entity_kind from suppliers
    '''
    social_service_entity_ids = fetch_from_budgetkey(query)
    regNums = sorted(set(
        row['entity_id'] for row in social_service_entity_ids
        if row['entity_id'] is not None
    ))
    regNums = [
        dict(id=id, data=dict(id=id, last_tag_date=today))
        for id in regNums
    ]

    print('COLLECTED {} relevant organizations'.format(len(regNums)))
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'entities',
        ['last_tag_date'],
        regNums, update_mapper(), 
        manage_status=False,
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )

def soprocServices(services):
    click_data = scrape_click()
    counter = 0
    for service in services:
        catalog_number = str(service['catalog_number']) if service['catalog_number'] is not None else None
        extra_data = deepcopy(click_data.get(catalog_number)) or dict()
        data_sources = extra_data.get('data_sources') or []
        if data_sources:
            data_sources = [data_sources]
        data_sources.append('https://www.socialpro.org.il/i/activities/gov_social_service/{}#דף השירות ב״מפתח לרכש החברתי״'.format(service['id']))
        data_sources = [
            '<a href="{}" target="_blank">{}</a>'.format(*ds.split('#', 1))# if 'href' not in ds else ds
            for ds in data_sources
        ]
        extra_data['data_sources'] = '\n'.join(data_sources)
        id = 'soproc:' + service['id']

        verifyDataExistance(service, counter)

        data = dict(
            description=service.get('description', ''),
            organizations=[s['entity_id'] for s in (service['suppliers'] or []) if s['active'] == 'yes'],
            urls=None,
        )
        data.update(extra_data)
        if service['office'] == 'משרד הרווחה':
            data['phone_numbers'] = '118'
        elif service['office'] == 'משרד הבריאות':
            data['phone_numbers'] = '*5400'
        data['soproc-service-tagging'] = id

        yield dict(
            id=id,
            data=data
        )

def fetchServiceData():
    print('FETCHING ALL SERVICES')
    query = '''
        select * from activities
    '''
    social_service_activities = list(fetch_from_budgetkey(query))
    print('COLLECTED {} relevant services'.format(len(social_service_activities)))

    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'social-procurement',
        ['description', 'details', 'payment_required', 'payment_details', 'urls', 'phone_numbers', 'organizations', 'data_sources', 'soproc-service-tagging'],
        soprocServices(social_service_activities),
        updateFromSourceData(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


def run(*_):
    logger.info('STARTING SoProc Scraping')
    logger.info('Updating Organizations')
    fetchOrgData()
    logger.info('Updating Services')
    fetchServiceData()
    logger.info("FINISHED SoProc Scraping")



def operator(*_):
    invoke_on(run, 'Social Procurement Data (Soproc)')


if __name__ == '__main__':
    run(None, None, None)
