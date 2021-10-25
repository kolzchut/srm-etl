from srm_tools.budgetkey import fetch_from_budgetkey
import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from srm_tools.logger import logger
from srm_tools.update_table import airflow_table_update_flow, airflow_table_updater
from srm_tools.situations import Situations

from conf import settings
from .click_scraper import scrape_click


situations = Situations()


## ORGANIZATIONS
def entities(regNums):
    for regNum in regNums:
        entity = list(fetch_from_budgetkey(f"select * from entities where id='{regNum}'"))
        if len(entity) > 0:
            entity = entity[0]
            name = entity['name']
            for x in (
                'בע״מ',
                 "בע'מ",
                 'ע״ר',
                 'חל״צ'
            ):
                name = name.replace(x, '')
            name = name.strip()
            rec = dict(
                id=entity['id'],
                data = dict(
                    name=name,
                    kind=entity['kind_he'],
                    purpose=entity['details'].get('goal'),
                )
            )
            yield rec


def companyBranches(regNums):
    for regNum in regNums:
        entity = list(fetch_from_budgetkey(f"select * from entities where id='{regNum}'"))
        if len(entity) > 0:
            entity = entity[0]
            name = entity['name']
            for x in (
                'בע״מ',
                 "בע'מ",
            ):
                name = name.replace(x, '')
            name = name.strip()
            rec = dict(
                id='budgetkey:' + entity['id'],
                data = dict(
                    name=name,
                    address=name,
                    location=[name],
                    organization=[entity['id']]
                )
            )
            yield rec


def updateFromSourceData():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        row.update(data)
    return func

def fetchOrgData():
    print('FETCHING ALL ORGANIZATIONS')
    query = '''
        with suppliers as (select jsonb_array_elements(suppliers) as supplier from activities where suppliers is not null and suppliers::text != 'null')
        select supplier->>'entity_id' as entity_id, supplier->>'entity_kind' as entity_kind from suppliers
    '''
    social_service_entity_ids = fetch_from_budgetkey(query)
    regNums = sorted(set(
        row['entity_id'] for row in social_service_entity_ids
        if row['entity_kind'] not in ('association', None)
    ))
    print('COLLECTED {} relevant organizations'.format(len(regNums)))
    airflow_table_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'budgetkey-entities',
        ['name', 'kind', 'purpose'],
        entities(regNums),
        updateFromSourceData()
    )

def fetchBranchData():
    print('FETCHING ALL BRANCHES')
    query = '''
        with suppliers as (select jsonb_array_elements(suppliers) as supplier from activities where suppliers is not null and suppliers::text != 'null')
        select supplier->>'entity_id' as entity_id, supplier->>'entity_kind' as entity_kind from suppliers
    '''
    social_service_entity_ids = fetch_from_budgetkey(query)
    regNums = sorted(set(
        row['entity_id'] for row in social_service_entity_ids
        if row['entity_kind'] in ('company', )
    ))
    print('COLLECTED {} relevant organizations'.format(len(regNums)))
    airflow_table_updater(settings.AIRTABLE_BRANCH_TABLE, 'budgetkey-entities',
        ['name', 'organization', 'address', 'location'],
        companyBranches(regNums),
        updateFromSourceData()
    )

def soprocServices(services):
    taxonomy = DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, 'Social Procurement Taxonomy Mapping', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    ).results()[0][0]
    taxonomy = dict(
        (r.pop('name'), r) for r in taxonomy
    )

    click_data = scrape_click()
    for service in services:
        catalog_number = str(service['catalog_number']) if service['catalog_number'] is not None else None
        click = click_data.get(catalog_number) or dict()
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
        data.update(click)
        data['situations'] = sorted(set([s for t in tags for s in (taxonomy[t]['situation_ids'] or [])] + (data.get('situations') or [])))
        data['responses'] = sorted(set([s for t in tags for s in (taxonomy[t]['response_ids'] or [])] + (data.get('responses') or [])))

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
    airflow_table_updater(settings.AIRTABLE_SERVICE_TABLE, 'social-procurement',
        ['name', 'description', 'details', 'location', 'payment_required', 'payment_details', 'urls', 'situations', 'responses', 'organizations'],
        soprocServices(social_service_activities),
        updateFromSourceData()
    )


def operator(name, params, pipeline):
    logger.info('STARTING Entity Scraping')
    fetchOrgData()
    fetchBranchData()
    fetchServiceData()


if __name__ == '__main__':
    operator(None, None, None)
    # query = '''
    #     select * from activities
    # '''
    # social_service_activities = fetch_from_budgetkey(query)
    # svc = list(soprocServices(social_service_activities))
    # print(svc[0])
    