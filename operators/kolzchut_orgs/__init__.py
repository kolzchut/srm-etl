import requests
import dataflows as DF
import datetime
from srm_tools.processors import update_mapper

from srm_tools.update_table import airtable_updater
from srm_tools.situations import Situations

from conf import settings
from srm_tools.logger import logger
from srm_tools.error_notifier import invoke_on


situations = Situations()


## ORGANIZATIONS
def fetchKZOrgs():
    URL = 'https://www.kolzchut.org.il/w/he/index.php?title=מיוחד:CargoExport&tables=organization&fields=organization_number&where=organization_number+is+not+null&format=json&limit=1000'
    regs = requests.get(URL).json()
    regs = sorted(set(str(r['organization number']) for r in regs))
    return regs


def fetchKZOrgData(*_):
    print('FETCHING ALL ORGANIZATIONS')
    today = datetime.date.today().isoformat()
    regNums = [
        dict(id=id, data=dict(id=id, last_tag_date=today))
        for id in fetchKZOrgs()
    ]

    print('COLLECTED {} relevant organizations'.format(len(regNums)))
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'entities',
        ['last_tag_date'],
        regNums, update_mapper(), 
        manage_status=False,
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )
    print('FINISHED KZ ORG DATA')


def operator(*_):
    logger.info('STARTING KZ Scraping')
    invoke_on(fetchKZOrgData, 'kolzchut_orgs')


if __name__ == '__main__':
    fetchKZOrgData()
