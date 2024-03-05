import datetime
import logging
import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from dataflows_airtable import dump_to_airtable, load_from_airtable, AIRTABLE_ID_FIELD

from openlocationcode import openlocationcode as olc

from srm_tools.update_table import airtable_updater
from srm_tools.situations import Situations

from conf import settings
from srm_tools.logger import logger
from srm_tools.guidestar_api import GuidestarAPI
from srm_tools.url_utils import fix_url


### GUIDESTAR IMPORT FLOW

## ORGANIZATIONS
def updateOrgFromSourceData():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        row['name'] = data['name'].replace(' (חל"צ)', '').replace(' (ע"ר)', '')
        if data.get('abbreviatedOrgName'):
            row['short_name'] = data['abbreviatedOrgName']
        row['kind'] = data['malkarType']
        row['description'] = None
        row['purpose'] = data.get('orgGoal')
        urls = []
        if data.get('website'):
            website = fix_url(data['website'])
            if website:
                urls.append(website + '#אתר הבית')
        row['urls'] = '\n'.join(urls)
        phone_numbers = []
        if data.get('tel1'):
            phone_numbers.append(data['tel1'])
        if data.get('tel2'):
            phone_numbers.append(data['tel2'])
        row['phone_numbers'] = '\n'.join(phone_numbers)
        if data.get('email'):
            row['email_address'] = data['email']
    return func

def fetchWildOrgData(ga: GuidestarAPI, skip_orgs):
    all_orgs = [
        org for org in ga.organizations() if org['id'] not in skip_orgs
    ]        
    print('COLLECTED {} relevant organizations'.format(len(all_orgs)))
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'guidestar',
        ['name', 'short_name', 'kind', 'urls', 'description', 'purpose', 'phone_numbers', 'email_address'],
        all_orgs,
        updateOrgFromSourceData(),
        airtable_base=settings.AIRTABLE_GUIDESTAR_IMPORT_BASE
    )
    return [org['id'] for org in all_orgs]

## BRANCHES
def unwind_branches(ga:GuidestarAPI):
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
                    if branch.get('placeNickname'):
                        branch['name'] = branch['placeNickname']
                    else:
                        branch['name'] = (row.get('short_name') or row.get('name')) + ' - ' + branch['cityName']
                    yield ret
                national = {}
                national.update(row)
                national['id'] = 'guidestar:' + regNum + ':national'
                national['data'] = {
                    'branchId': national['id'],
                    'organization_id': regNum,
                    'name': row['name'],
                    'address': 'שירות ארצי',
                    'location': 'שירות ארצי',
                }
                yield national
    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
    )


def calc_address(row):
    if row.get('address'):
        return row['address']
    key = ''
    cityName = row.get('cityName')
    if cityName:
        cityName = cityName.replace(' תאי דואר', '')
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

def calc_location_key(src, dst):
    y, x = src.get('latitude'), src.get('longitude')
    if y and x:
        code = olc.encode(y, x, 11)
    else:
        code = None
    return code or dst['address']

def updateBranchFromSourceData():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        # print('data', data)
        # print('row', row)
        row['name'] = data['name']
        row['address'] = calc_address(data)
        row['location'] = calc_location_key(data, row)
        row['address_details'] = data.get('drivingInstructions')
        row['description'] = None
        row['urls'] = None
        # if data.get('branchURL'):
        #     row['urls'] = data['branchURL'] + '#הסניף בגיידסטאר'
        row['phone_numbers'] = None
        if data.get('phone'):
            row['phone_numbers'] = data['phone']
        if row.get('organization_id'):
            row['organization'] = [row['organization_id']]
        if data.get('language'):
            row['situations'] = [
                'human_situations:language:{}_speaking'.format(l.lower().strip()) for l in data['language'].split(';')
            ]
    return func


def fetchWildBranchData(ga):
    print('FETCHING ALL ORGANIZATION BRANCHES')
    airtable_updater(settings.AIRTABLE_BRANCH_TABLE, 'guidestar',
        ['name', 'organization', 'address', 'address_details', 'location', 'description', 'phone_numbers', 'urls', 'situations'],
        DF.Flow(
            load_from_airtable(settings.AIRTABLE_GUIDESTAR_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='orgs'),
            DF.filter_rows(lambda r: r['source'] == 'guidestar', resources='orgs'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
            DF.rename_fields({
                AIRTABLE_ID_FIELD: 'organization_id',
            }, resources='orgs'),
            DF.select_fields(['organization_id', 'id', 'name', 'short_name'], resources='orgs'),
            unwind_branches(ga),
        ),
        updateBranchFromSourceData(),
        airtable_base=settings.AIRTABLE_GUIDESTAR_IMPORT_BASE
    )

## Services

def fetchWildServiceData(ga, taxonomy):
    print('FETCHING ALL ORGANIZATION SERVICES')
    existing_orgs = set()

    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'guidestar',
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'responses',
         'organizations', 'branches', 'data_sources', 'implements', 'phone_numbers', 'email_address'],
        DF.Flow(
            load_from_airtable(settings.AIRTABLE_GUIDESTAR_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='orgs'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
            DF.select_fields(['id', 'name', 'source'], resources='orgs'),
            unwind_services(ga, source='guidestar'),
            # DF.checkpoint('unwind_services'),
        ),
        updateServiceFromSourceData(taxonomy),
        airtable_base=settings.AIRTABLE_GUIDESTAR_IMPORT_BASE
    )

    return existing_orgs


def operator(name, params, pipeline):
    logger.info('STARTING Guidestar Scraping')
    ga = GuidestarAPI()

    skip_orgs = fetchServiceData(ga, taxonomy)

    logger.info('FETCHING Org data for wild organizations')
    fetchWildOrgData(ga, skip_orgs)
    fetchWildBranchData(ga)
    fetchWildServiceData(ga, taxonomy)


if __name__ == '__main__':
    import logging
    import sys
    logger.setLevel(logging.DEBUG)
    logger.info('STARTING Guidestar Scraping')
    logger.addHandler(logging.StreamHandler(sys.stdout))
    # operator(None, None, None)
    ga = GuidestarAPI()
    orgs = [
        dict(source='entities', id='580019800'),
    ]
    DF.Flow(
        orgs,
        DF.update_resource(-1, name='orgs'),
        unwind_services(ga),
        updateServiceFromSourceData(taxonomy),
        DF.printer(),
    ).process()
