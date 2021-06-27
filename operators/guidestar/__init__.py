import dataflows as DF
import logging

from guidestar_api import GuidestarAPI
from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD


def unwind_branches(ga, rows):
    for row in rows:
        regNum = row['regNum']
        branches = ga.branches(regNum)
        if len(branches) > 10:
            print(len(branches), regNum)
        for branch in branches:
            ret = dict(streetCode=None, streetName=None, houseNum=None)
            ret.update(row)
            ret.update(branch)
            yield ret


def operator(name, params, pipeline):
    logging.info('STARTING Guidestar Scraping')
    ga = GuidestarAPI()
    DF.Flow(
        load_from_airtable('appF3FyNsyk4zObNa', 'Guidestar Mirror', 'Grid view'),
        DF.update_resource(-1, name='current'),
        unwind_branches(ga, ga.organizations(limit=50)),
        DF.update_resource(-1, name='guidestar'),
        DF.join('current', ['branchId'], 'guidestar', ['branchId'], {
            AIRTABLE_ID_FIELD: None
        }),
        # DF.set_type('language', type='array', transform=lambda v: v.split(';')),
        DF.set_type('hasReception', type='boolean'),
        DF.set_type('primaryClassifications', type='string', transform=lambda v: ';'.join(v)),
        DF.set_type('secondaryClassifications', type='string', transform=lambda v: ';'.join(v)),
        DF.delete_fields([
            'hasGovSupports3Years', 'hasGovServices3Years', 'volunteerProjectId',
            'secondaryClassificationsNums', 'primaryClassificationsNums', 
            'hasReportsLast3Years', 'hasNihulTakin', 'lastModifiedDate',
            'cityCode', 'streetCode', 'urlGuidestar', 'branchCount', 'orgYearFounded',
            'malkarStatus', 'malkarType', 'regNum', 'services']),
        DF.printer(),
        dump_to_airtable({
            ('appF3FyNsyk4zObNa', 'Guidestar Mirror'): {
                'resource-name': 'guidestar'
            }

        })
    ).process()


if __name__ == '__main__':
    operator(None, None, None)
