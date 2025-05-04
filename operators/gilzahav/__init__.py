import dataflows as DF
import shutil

import requests

from conf import settings
from srm_tools.logger import logger
from srm_tools.processors import update_mapper
from srm_tools.update_table import airtable_updater
from srm_tools.error_notifier import invoke_on


DATA_SOURCE_ID = 'gilzahav'

GZ_URL = 'https://www.gov.il/api/moch/viewlist/Loaddata/ViewList/gil_zahav'
GZ_WEBSITE = 'https://www.gov.il/apps/moch/viewlist/list/gil_zahav'

CHECKPOINT = DATA_SOURCE_ID
ORG_ID = '500100797'

SERVICE_DESCRIPTION = '''משרד הבינוי והשיכון מנהל כ-120 בתי דיור לגיל הזהב הכוללים כ-12,000 דירות, שמהן בדרך כלל שני שלישים דירות בנות חדר וחצי, ושליש דירות בנות שני חדרים. בכל מבנה יש דירה המיועדת לאם הבית וכן מועדון, חדרי חוגים וגינה מטופחת. את בתי הדיור מוביל צוות תחזוקה שבראשו עומדת אם הבית. רוב בתי הדיור מנוהלים בעבור המשרד על ידי חברות ממשלתיות וחלקם על ידי חברות פרטיות.'''
SERVICE_DETAILS = '''- סדר השיבוץ לבית דיור נקבע לפי נוהלי משרד הבינוי והשיכון, ובכלל זאת לפי מועדי קבלת הזכאות והכניסה לתור הממתינים.
- הנתונים משקפים את המידע הנמצא במערכות משרד הבינוי והשיכון בלבד, והם אינם מתבססים על רשימת הממתינים שמנהל בנפרד משרד הקליטה.

'''

SERVICE = {
    'id': 'gilzahav',
    'data': {
        'name': 'בית דיור גיל הזהב',
        'description': SERVICE_DESCRIPTION,
        'details': SERVICE_DETAILS,
        'phone_numbers': None,
        'urls': GZ_WEBSITE,
        'data_sources': f'המידע התקבל מ<a target="_blank" href="{GZ_WEBSITE}" target="_blank">אתר משרד הבינוי והשיכון</a>',
        'responses': [
            'human_services:housing:residential_housing:long_term_housing',
            'human_services:housing:residential_housing:long_term_housing:old_age_home'
        ],
        'situations': [
            'human_situations:age_group:seniors',
            'human_situations:benefit_holders:holocaust_survivors',
        ]
    },
}

def branch_description(r):
    d = f'מספר יחידות דיור: {r["field3"]}\n'
    if r['field8'] not in (None, '-'):
        d += f'מספר ממתינים: {r["field8"]}\n'
    return d


FIELD_MAP = {
    'id': {'transform': lambda r: f'{DATA_SOURCE_ID}:{r["field9"]}'},
    # 'source': {'transform': lambda r: DATA_SOURCE_ID},
    'name': {'transform': lambda r: r["field2"]},
    'organization': {'type': 'array', 'transform': lambda r: [ORG_ID]},
    'services': {'type': 'array', 'transform': lambda r: [SERVICE['id']]},
    'description': {'transform': branch_description},
    'phone_numbers': 'field6',
    'email_address': 'field7',
    'address': {'transform': lambda r: f'{r["field5"]}, {r["field1"]}'},
    'location': {'transform': lambda r: f'{r["field5"]}, {r["field1"]}'},
    'operating_unit': 'field4'
}


def ensure_field(name, args, resources=None):
    args = {'source': args} if isinstance(args, str) else args
    name, source, type, transform = (
        name,
        args.get('source', None),
        args.get('type', 'string'),
        args.get('transform', lambda r: r.get(source) if source else None),
    )
    return DF.add_field(name, type, transform, resources=resources)


def gz_service_data_flow():
    return airtable_updater(
        settings.AIRTABLE_SERVICE_TABLE,
        DATA_SOURCE_ID,
        list(SERVICE['data'].keys()),
        [SERVICE],
        update_mapper(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


def gz_branch_data_flow(data):
    airtable_updater(
        settings.AIRTABLE_BRANCH_TABLE,
        DATA_SOURCE_ID,
        list(FIELD_MAP.keys()),
        DF.Flow(
            data,
            DF.update_resource(name='branches', path='branches.csv', resources=-1),
            *[ensure_field(key, args, resources=['branches']) for key, args in FIELD_MAP.items()],
            DF.select_fields(list(FIELD_MAP.keys()), resources=['branches']),
            DF.add_field(
                'data',
                'object',
                lambda r: {k: v for k, v in r.items() if not k in ('id', 'source', 'status')},
                resources=['branches'],
            ),
            DF.select_fields(['id', 'data'], resources=['branches']),
        ),
        update_mapper(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )

def get_gz_data():
    session = requests.Session()
    session.headers.update({'Referer': 'https://www.gov.il/apps/moch/viewlist/list/gil_zahav'})
    return DF.Flow(
        DF.load(GZ_URL, format='json', http_session=session),
        DF.checkpoint(CHECKPOINT),
        DF.set_type('field1', transform=lambda v: '-'.join(v.split('-')[:-1])),
        DF.printer()
    ).results()[0][0]


def runFlow(*_):
    logger.info('Starting GilZahav Flow')

    shutil.rmtree(f'.checkpoints/{CHECKPOINT}', ignore_errors=True, onerror=None)

    gz_service_data_flow()
    DATA = get_gz_data()
    gz_branch_data_flow(DATA)

    logger.info('Finished GilZahav Flow')

def operator(*_):
    invoke_on(runFlow, 'GilZahav')

if __name__ == '__main__':
    operator(None, None, None)
