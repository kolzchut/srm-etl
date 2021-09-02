from srm_tools.budgetkey import fetch_from_budgetkey
import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from srm_tools.logger import logger
from srm_tools.update_table import airflow_table_update_flow, airflow_table_updater
from srm_tools.situations import Situations

from conf import settings


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
        with suppliers as (select jsonb_array_elements(suppliers) as supplier from activities where suppliers is not null and suppliers::text != 'null' and catalog_number is not null)
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
        with suppliers as (select jsonb_array_elements(suppliers) as supplier from activities where suppliers is not null and suppliers::text != 'null' and catalog_number is not null)
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

def operator(name, params, pipeline):
    logger.info('STARTING Entity Scraping')
    fetchOrgData()
    fetchBranchData()


if __name__ == '__main__':
    operator(None, None, None)
