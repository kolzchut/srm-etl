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
    airflow_table_updater('Organizations', 'guidestar', 
        ['name', 'kind', 'urls', 'description', 'purpose'],
        ga.organizations(),
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
                'language/{}'.format(l.lower().strip()) for l in data['language'].split(';')
            ])

    return func


def fetchBranchData(ga):
    print('FETCHING ALL ORGANIZATION BRANCHES')
    DF.Flow(
        DF.update_resource(-1, name='locations'),
        DF.rename_fields({
            AIRTABLE_ID_FIELD: 'location',
        }, resources='locations'),
        DF.set_type('location', type='array', transform=lambda x: [x], resources='locations'),

        airflow_table_update_flow('Branches', 'guidestar',
            ['name', 'organization', 'address', 'address_details', 'description', 'phone_numbers', 'urls', 'situations'],
        load_from_airtable(
            settings.AIRTABLE_BASE,
            settings.AIRTABLE_LOCATION_TABLE,
            settings.AIRTABLE_VIEW,
        ),
            DF.Flow(
                load_from_airtable('appF3FyNsyk4zObNa', 'Organizations', 'Grid view'),
                DF.update_resource(-1, name='orgs'),
                DF.filter_rows(lambda r: r['source'] == 'guidestar', resources='orgs'),
                DF.rename_fields({
                    AIRTABLE_ID_FIELD: 'organization_id',
                }, resources='orgs'),
                DF.select_fields(['organization_id', 'id', 'name'], resources='orgs'),
                load_from_airtable(
                    settings.AIRTABLE_BASE,
                    settings.AIRTABLE_ORGANIZATION_TABLE,
                    settings.AIRTABLE_VIEW,
                ),
                unwind_branches(ga),
            ),
            DF.Flow(
                updateBranchFromSourceData(),
                DF.join('locations', ['key'], 'fetched', ['address'], dict(
                    location=None
                )),
            )
        )
    ).process()


## LOCATIONS
def updateLocations():
    print('UPDATING LOCATION TABLE WITH NEW LOCATIONS')
    DF.Flow(
        DF.update_resource(-1, name='locations'),

        DF.update_resource(-1, name='guidestar'),
        DF.filter_rows(lambda r: r['source'] == 'guidestar', resources='guidestar'),

        DF.select_fields(['address'], resources='guidestar'),
        DF.rename_fields({
            'address': 'key',
        }, resources='guidestar'),

        DF.join('locations', ['key'], 'guidestar', ['key'], {
            AIRTABLE_ID_FIELD: None
        }),
        load_from_airtable(
            settings.AIRTABLE_BASE,
            settings.AIRTABLE_LOCATION_TABLE,
            settings.AIRTABLE_VIEW,
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE,
            settings.AIRTABLE_BRANCH_TABLE,
            settings.AIRTABLE_VIEW,
        ),
        DF.filter_rows(lambda r: r[AIRTABLE_ID_FIELD] is None),
        DF.join_with_self('guidestar', ['key'], {
            'key': None, AIRTABLE_ID_FIELD: None
        }),

        dump_to_airtable(
            {
                (settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE): {
                    "resource-name": "guidestar",
                    "typecast": True,
                }
            }
        ),
    ).process()


def operator(name, params, pipeline):
    logger.info('STARTING Guidestar Scraping')
    ga = GuidestarAPI()

    # fetchOrgData(ga)
    fetchBranchData(ga)

    updateLocations()  # TODO: Move to centralized place


if __name__ == '__main__':
    operator(None, None, None)
