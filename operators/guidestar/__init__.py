import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from datapackage import resource

from .guidestar_api import GuidestarAPI
from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from srm_tools.logger import logger


def unwind_branches(ga):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'guidestar':
            yield from rows        
        else:
            for i, row in enumerate(rows):
                regNum = row['regNum']
                branches = ga.branches(regNum)
                for branch in branches:
                    ret = dict()
                    ret.update(row)
                    ret.update(branch)
                    yield ret
    return DF.Flow(
        DF.add_field('branchId', 'string', resources='guidestar'),
        DF.add_field('branchGuidestarURL', 'string', resources='guidestar'),
        DF.add_field('type', 'string', resources='guidestar'),
        DF.add_field('language', 'string', resources='guidestar'),
        DF.add_field('phone', 'string', resources='guidestar'),
        DF.add_field('branchURL', 'string', resources='guidestar'),
        DF.add_field('hasReception', 'boolean', resources='guidestar'),
        DF.add_field('district', 'string', resources='guidestar'),
        DF.add_field('cityName', 'string', resources='guidestar'),
        DF.add_field('streetName', 'string', resources='guidestar'),
        DF.add_field('houseNum', 'string', resources='guidestar'),
        DF.add_field('drivingInstructions', 'string', resources='guidestar'),
        DF.add_field('alternateAddress', 'string', resources='guidestar'),
        func,
        DF.delete_fields(['regNum'], resources='guidestar'),
    )

def calc_location_key(row):
    key = ''
    cityName = row['cityName']
    if cityName:
        streetName = row['streetName']
        if streetName:
            key += f'{streetName} '
            houseNum = row['houseNum']
            if houseNum:
                key += f'{houseNum} '
            key += ', '
        key += f'{cityName} '
    
    alternateAddress = row['alternateAddress']
    if alternateAddress:
        if alternateAddress not in key:
            key += f' - {alternateAddress}'
    key = key.strip()

    return key or None
    

def operator(name, params, pipeline):
    logger.info('STARTING Guidestar Scraping')
    ga = GuidestarAPI()

    print('FETCHING ALL ORGANIZATIONS')
    DF.Flow(
        load_from_airtable('appF3FyNsyk4zObNa', 'Guidestar Mirror', 'Grid view'),
        DF.update_resource(-1, name='current'),
        ga.organizations(),
        DF.update_resource(-1, name='guidestar'),
        DF.join('current', ['regNum'], 'guidestar', ['regNum'], {
            AIRTABLE_ID_FIELD: None
        }),
        DF.set_type('primaryClassifications', type='array'),
        DF.set_type('secondaryClassifications', type='array'),
        DF.delete_fields(['hasGovSupports3Years', 'hasGovServices3Years', 'branchCount', 'volunteerProjectId', 'secondaryClassificationsNums', 'primaryClassificationsNums', 
            'hasReportsLast3Years', 'hasNihulTakin', 'lastModifiedDate']),
        DF.printer(),
        dump_to_airtable({
            ('appF3FyNsyk4zObNa', 'Guidestar Mirror'): {
                'resource-name': 'guidestar',
                'typecast': True
            }
        })
    ).process()

    print('FETCHING ALL ORGANIZATION BRANCHES')
    DF.Flow(
        load_from_airtable('appF3FyNsyk4zObNa', 'Locations', 'Grid view'),
        DF.update_resource(-1, name='locations'),
        DF.rename_fields({
            AIRTABLE_ID_FIELD: 'location',
        }, resources='locations'),
        DF.set_type('location', type='array', transform=lambda x: [x], resources='locations'),

        load_from_airtable('appF3FyNsyk4zObNa', 'Guidestar Branches Mirror', 'Grid view'),
        DF.update_resource(-1, name='current'),
        load_from_airtable('appF3FyNsyk4zObNa', 'Guidestar Mirror', 'Grid view'),
        DF.update_resource(-1, name='guidestar'),
        DF.rename_fields({
            AIRTABLE_ID_FIELD: 'organization',
        }, resources='guidestar'),
        DF.set_type('organization', type='array', transform=lambda x: [x], resources='guidestar'),
        DF.select_fields(['organization', 'regNum'], resources='guidestar'),
        
        unwind_branches(ga),
        DF.join('current', ['branchId'], 'guidestar', ['branchId'], {
            AIRTABLE_ID_FIELD: None
        }),

        DF.add_field('location_key', 'string', calc_location_key, resources='guidestar'),
        DF.delete_fields(['cityName', 'streetName', 'houseNum', 'alternateAddress'], resources='guidestar'),
        DF.join('locations', ['key'], 'guidestar', ['location_key'], dict(
            location=None
        )),

        DF.set_type('language', type='array', transform=lambda v: v.split(';')),
        # update_services(),
        DF.printer(),
        dump_to_airtable({
            ('appF3FyNsyk4zObNa', 'Guidestar Branches Mirror'): {
                'resource-name': 'guidestar',
                'typecast': True
            }
        })
    ).process()

    print('UPDATING LOCATION TABLE WITH NEW LOCATIONS')
    DF.Flow(
        load_from_airtable('appF3FyNsyk4zObNa', 'Locations', 'Grid view'),
        DF.update_resource(-1, name='locations'),

        load_from_airtable('appF3FyNsyk4zObNa', 'Guidestar Branches Mirror', 'Grid view'),
        DF.update_resource(-1, name='guidestar'),
        DF.select_fields(['location_key'], resources='guidestar'),
        DF.rename_fields({
            'location_key': 'key',
        }, resources='guidestar'),

        DF.join('locations', ['key'], 'guidestar', ['key'], {
            AIRTABLE_ID_FIELD: None
        }),
        DF.filter_rows(lambda r: r[AIRTABLE_ID_FIELD] is None),
        DF.join_with_self('guidestar', ['key'], {
            'key': None, AIRTABLE_ID_FIELD: None
        }),

        dump_to_airtable({
            ('appF3FyNsyk4zObNa', 'Locations'): {
                'resource-name': 'guidestar',
                'typecast': True
            }
        })
    ).process()


if __name__ == '__main__':
    operator(None, None, None)
