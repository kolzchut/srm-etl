import datetime
from srm_tools.budgetkey import fetch_from_budgetkey
import dataflows as DF

from srm_tools.logger import logger
from srm_tools.processors import update_mapper
from srm_tools.update_table import airtable_updater
from srm_tools.situations import Situations

from conf import settings
from .click_scraper import scrape_click


situations = Situations()


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
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )

def soprocServices(services):
    click_data = scrape_click()
    for service in services:
        catalog_number = str(service['catalog_number']) if service['catalog_number'] is not None else None
        extra_data = click_data.get(catalog_number) or dict(
            urls='https://www.socialpro.org.il/i/activities/gov_social_service/{}#דף השירות ב״מפתח לרכש החברתי״'.format(service['id']),
        )
        id = 'soproc:' + service['id']
        tags = (
            (service['intervention'] or []) +
            (service['subject'] or []) +
            (service['target_age_group'] or []) +
            (service['target_audience'] or [])
        )
        data = dict(
            name=service['name'],
            description=service['description'],
            organizations=[s['entity_id'] for s in (service['suppliers'] or [])],
        )
        data.update(extra_data)

        yield dict(
            id=id,
            tags=tags,
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
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'organizations'],
        soprocServices(social_service_activities),
        updateFromSourceData(),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )


def operator(name, params, pipeline):
    logger.info('STARTING Entity Scraping')
    fetchOrgData()
    fetchServiceData()


if __name__ == '__main__':
    operator(None, None, None)
    # query = '''
    #     select * from activities
    # '''
    # social_service_activities = fetch_from_budgetkey(query)
    # svc = list(soprocServices(social_service_activities))
    # print(svc[0])
    