import requests
import dataflows as DF
import datetime

from srm_tools.update_table import airtable_updater, simple_row_updater
from srm_tools.situations import Situations

from conf import settings
from srm_tools.logger import logger


situations = Situations()


## ORGANIZATIONS
def fetchKZOrgs():
    URL = 'https://www.kolzchut.org.il/w/he/index.php?title=מיוחד:CargoExport&tables=organization&fields=organization_number&where=organization_number+is+not+null&format=json&limit=1000'
    regs = requests.get(URL).json()
    regs = sorted(set(str(r['organization number']) for r in regs))
    return regs


def fetchKZOrgData():
    print('FETCHING ALL ORGANIZATIONS')
    today = datetime.date.today().isoformat()
    regNums = [
        dict(id=id, data=dict(id=id, last_tag_date=today))
        for id in fetchKZOrgs()
    ]

    print('COLLECTED {} relevant organizations'.format(len(regNums)))
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'entities',
        ['last_tag_date'],
        regNums, simple_row_updater(), 
        manage_status=False
    )


def operator(name, params, pipeline):
    logger.info('STARTING KZ Scraping')
    fetchKZOrgData()


if __name__ == '__main__':
    operator(None, None, None)
