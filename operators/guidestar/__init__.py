from srm_tools.budgetkey import fetch_from_budgetkey
import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

try:
    from .guidestar_api import GuidestarAPI
except ImportError:
    from guidestar_api import GuidestarAPI

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from srm_tools.logger import logger
from srm_tools.update_table import airflow_table_update_flow, airflow_table_updater
from srm_tools.situations import Situations

from conf import settings


situations = Situations()


## ORGANIZATIONS
def updateOrgFromSourceData():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        row['name'] = data['name'].replace(' (חל"צ)', '').replace(' (ע"ר)', '')
        row['kind'] = data['malkarType']
        row['description'] = None
        row['purpose'] = data.get('orgGoal')
        urls = []
        if data.get('website'):
            urls.append(data['website'] + '#אתר הבית')
        if data.get('urlGuidestar'):
            urls.append(data['urlGuidestar'] + '#הארגון בגיידסטאר')
        row['urls'] = '\n'.join(urls)
    return func

def fetchOrgData(ga):
    print('FETCHING ALL ORGANIZATIONS')
    query = '''
        with suppliers as (select jsonb_array_elements(suppliers) as supplier from activities where suppliers is not null and suppliers::text != 'null' and catalog_number is not null)
        select supplier->>'entity_id' as entity_id, supplier->>'entity_kind' as entity_kind from suppliers
    '''
    social_service_entity_ids = fetch_from_budgetkey(query)
    regNums = sorted(set(
        row['entity_id'] for row in social_service_entity_ids
        if row['entity_kind'] == 'association'
    ))
    print('COLLECTED {} relevant organizations'.format(len(regNums)))
    airflow_table_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'guidestar',
        ['name', 'kind', 'urls', 'description', 'purpose'],
        ga.organizations(regNums=regNums),
        updateOrgFromSourceData()
    )


## BRANCHES
def unwind_branches(ga):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows        
        else:
            for _, row in enumerate(rows):
                regNum = row['id']
                branches = ga.branches(regNum)
                for branch in branches:
                    ret = dict()
                    ret.update(row)
                    ret['data'] = branch
                    ret['id'] = 'guidestar:' + branch['branchId']
                    branch['name'] = row['name'] + ' - ' + branch['cityName']
                    yield ret
    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
    )

def calc_location_key(row):
    key = ''
    cityName = row.get('cityName')
    if cityName:
        streetName = row.get('streetName')
        if streetName:
            key += f'{streetName} '
            houseNum = row.get('houseNum')
            if houseNum:
                key += f'{houseNum} '
            key += ', '
        key += f'{cityName} '
    
    alternateAddress = row.get('alternateAddress')
    if alternateAddress:
        if alternateAddress not in key:
            key += f' - {alternateAddress}'
    key = key.strip()

    return key or None

def updateBranchFromSourceData():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        # print('data', data)
        # print('row', row)
        row['name'] = data['name']
        row['address'] = calc_location_key(data)
        row['location'] = [row['address']]
        row['address_details'] = data.get('drivingInstructions')
        row['description'] = None
        row['urls'] = None
        if data.get('branchURL'):
            row['urls'] = data['branchURL'] + '#הסניף בגיידסטאר'
        row['phone_numbers'] = None
        if data.get('phone'):
            row['phone_numbers'] = data['phone']
        if row.get('organization_id'):
            row['organization'] = [row['organization_id']]
        if data.get('language'):
            row['situations'] = situations.convert_situation_list([
                'human_situations:language:{}_speaking'.format(l.lower().strip()) for l in data['language'].split(';')
            ])

    return func


def fetchBranchData(ga):
    print('FETCHING ALL ORGANIZATION BRANCHES')
    DF.Flow(
        airflow_table_update_flow('Branches', 'guidestar',
            ['name', 'organization', 'address', 'address_details', 'location', 'description', 'phone_numbers', 'urls', 'situations'],
            DF.Flow(
                load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW),
                DF.update_resource(-1, name='orgs'),
                DF.filter_rows(lambda r: r['source'] == 'guidestar', resources='orgs'),
                DF.rename_fields({
                    AIRTABLE_ID_FIELD: 'organization_id',
                }, resources='orgs'),
                DF.select_fields(['organization_id', 'id', 'name'], resources='orgs'),
                unwind_branches(ga),
            ),
        )
    ).process()


def operator(name, params, pipeline):
    logger.info('STARTING Guidestar Scraping')
    ga = GuidestarAPI()

    fetchOrgData(ga)
    fetchBranchData(ga)


if __name__ == '__main__':
    operator(None, None, None)
