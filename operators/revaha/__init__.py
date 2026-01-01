import hashlib
from http.client import PAYMENT_REQUIRED
from pathlib import Path
import re
import time

import dataflows as DF
import requests
import slugify

from dataflows_airtable import load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from conf import settings
from srm_tools.datagovil import fetch_datagovil_datastore
from srm_tools.logger import logger
from srm_tools.processors import ensure_fields, update_mapper
from srm_tools.update_table import airtable_updater
from srm_tools.error_notifier import invoke_on

def transform_phone_numbers(r):
    phone_numbers = (r['authority_phone'] or '').split(',')
    machlaka_phone = (r['machlaka_phone'] or '').split(',')
    phone_numbers = [*phone_numbers, *machlaka_phone]
    phone_numbers = '\n'.join(phone_numbers)
    return phone_numbers.replace(' ', '')


def transform_email_addresses(r):
    pattern = r'[\w.+-]+@[\w-]+\.[\w.-]+'
    match = re.search(pattern, r['email']) if r['email'] else None
    return match.group(0) if match else None


def sort_dict_by_keys(row):
    return dict(sorted(row.items(), key=lambda i: i[0]))


BASE_URL = (
    'https://www.gov.il/he/departments/dynamiccollectors/molsa-social-departmentsd-list?skip=0'
)

DATA_SOURCE_ID = 'revaha'
DATA_SOURCES = f'המידע מ<a target="_blank" href="{BASE_URL}" target="_blank">אתר משרד הרווחה</a>'
PAYMENT_DETAILS = 'נדרש תיאום מראש'

BRANCH_NAME_PREFIX = 'מחלקה לשירותים חברתיים'
EMERGENCY_DESCRIPTION = '\n\n' + 'בזמן המלחמה המחלקה לשירותים חברתיים מעניקה סיוע, ייעוץ והכוונה גם למי שפונה ונמצא באזור המגורים בה פועלת המחלקה.'
EMERGENCY_TAG = 'human_services:internal_emergency_services'
ORG_ID = '500106406'

SERVICES = [
    {
        'id': 'revaha-aid',
        'data': {
            'name': 'תמיכה וייעוץ ליחידים ומשפחות מטעם המחלקה לשירותים חברתיים',
            'source': DATA_SOURCE_ID,
            'description': 'השירות מסייע לילדים, בני נוער, משפחות, יחידים, מוגבלים, זקנים, עולים חדשים ולכל פרט/קבוצה החפצים בקבלת סיוע. המחלקות לשירותים חברתיים  מעניקות מידע, יעוץ, טיפול, שירותים סוציאליים, הכוונה, תיווך לקבלת שירות, שילוב במסגרות ושירותי עזר בבית - בהתאם לכללי נזקקות וזכאות ולאפשרויות התקציביות.' + EMERGENCY_DESCRIPTION,
            'payment_required': 'no',
            'urls': f'{BASE_URL}#{BRANCH_NAME_PREFIX}',
            'organizations': [],
            'payment_details': PAYMENT_DETAILS,
            'data_sources': DATA_SOURCES,
            'responses': [
                'human_services:place:welfare_bureau',
                'human_services:food',
                'human_services:care',
                'human_services:legal:advocacy_legal_aid:understand_government_programs',
                EMERGENCY_TAG
            ],
            'situations': []
        }
    },
    {
        'id': 'revaha-seniors',
        'data': {
            'name': 'סיוע לאזרחים ותיקים מטעם המחלקה לשירותים חברתיים',
            'source': DATA_SOURCE_ID,
            'description': 'השירות לאוכלוסיית התושבים הוותיקים ובני משפחותיהם ניתן במחלקות לשירותים חברתיים וכולל מיצוי זכויות, מידע על מסגרות יומיות ,שירותי סעד, עובדים זרים, דיור מוגן ומסגרות מוסדיות זמניות וקבועות.' + EMERGENCY_DESCRIPTION,
            'payment_required': 'no',
            'urls': f'{BASE_URL}#{BRANCH_NAME_PREFIX}',
            'organizations': [],
            'payment_details': PAYMENT_DETAILS,
            'data_sources': DATA_SOURCES,
            'responses': [
                'human_services:place:welfare_bureau',
                'human_services:care',
                EMERGENCY_TAG
            ],
            'situations': [
                'human_situations:age_group:seniors',
            ]
        },
    },
    {
        'id': 'revaha-disabilities',
        'data': {
            'name': 'סיוע לאנשים עם מוגבלות מטעם המחלקה לשירותים חברתיים',
            'source': DATA_SOURCE_ID,
            'description': 'השירות לאנשים  עם מוגבלות ובני משפחותיהם ניתן במחלקות לשירותים חברתיים ומיועד לאנשים עם פיגור שכלי, אוטיזם, מוגבלויות פיזיות וחושיות (עיוורון וחירשות) ולבני משפחותיהם. למימוש הזכאות לשירותים יש צורך בהכרה של משרד הרווחה והשירותים החברתיים.' + EMERGENCY_DESCRIPTION,
            'payment_required': 'no',
            'urls': f'{BASE_URL}#{BRANCH_NAME_PREFIX}',
            'organizations': [],
            'payment_details': PAYMENT_DETAILS,
            'data_sources': DATA_SOURCES,
            'responses': [
                'human_services:place:welfare_bureau',
                'human_services:care',
                EMERGENCY_TAG
            ],
            'situations': [
                'human_situations:disability',
                'human_situations:benefit_holders:social_security',
            ]
        },
    },
]

SERVICE_MAP = {
    'noshmim': {
        'id': 'revaha-noshmim',
        'data': {
            'name': 'נושמים לרווחה',
            'source': DATA_SOURCE_ID,
            'description': '''מטרות תוכנית 'נושמים לרווחה' הן:

-  שיפור המצב הכלכלי של משפחות החיות בעוני ובהדרה (הרחקה חברתית) והעלאת העצמאות ביכולת לספק את הצרכים הבסיסיים
- הגדלת ההזדמנויות של המשפחה להגיע למגוון שירותים, מצבים ופעולות בחברה שעשויים לקדם א מצבם החברתי-כלכלי (לדוגמה רמת בתי ספר נמוכה בישוב או העדר מסגרות להכשרה מקצועית, קושי לקבל משכנתא או הלוואה בריבית נמוכה, העדר תחבורה ציבורית ועוד)
- העשרת הידע והמוניטין המגדירים את מעמד המשפחה ("הון סימבולי")
- יצירת תחושת רווחה אישית ומשפחתית.

התוכנית מיועדת ליחידים ולמשפחות שמתמודדות עם מכשולים רבים, מיעוט הזדמנויות לשנות את מצבם וקושי במיצוי זכויות ובשימוש במשאבי הקהילה והממסד.
''',
            'payment_required': 'no',
            # 'urls': 'https://clickrevaha.molsa.gov.il/product-page/178',
            'organizations': [],
            'payment_details': 'יש לפנות לעובד/ת הסוציאלית של המשפחה',
            'data_sources': DATA_SOURCES,
            'responses': [
                'human_services:place:welfare_bureau',
                'human_services:legal:advocacy_legal_aid:understand_government_programs',
                'human_services:money:financial_assistance',
                'human_services:education:more_education:financial_education',
                'human_services:care:guidance',
            ],
            'situations': [
                'human_situations:household:families',
                'human_situations:deprivation:low_income',
            ]
        }
    },
    'otzma': {
        'id': 'revaha-otzma',
        'data': {
            'name': 'מרכז עוצמה',
            'source': DATA_SOURCE_ID,
            'description': '''מטרת השירות היא הנגשת סיוע ליחידים ולמשפחות החיים בעוני, בדרכים הבאות:

- שיפור באיכות החיים
- הפחתת רמת המחסור החומרי-כלכלי
- העלאת רמת התעסוקה, הכשרה או השכלה ומיצוי זכויות
- הגדלת ההון הסימבולי - המתבטא בשילוב מיטבי בקהילה
- הגברת העצמאות, הבאה לידי ביטוי ביכולת לספק צרכים בסיסיים.

השירות מיועד ליחידים ולמשפחות, המוכרים למחלקות לשירותים חברתיים, המתמודדים עם מציאות חיים של עוני והדרה.
''',
            'payment_required': 'no',
            # 'urls': 'https://clickrevaha.molsa.gov.il/product-page/179',
            'organizations': [],
            'payment_details': 'יש לפנות למחלקה לשירותים חברתיים הסמוכה למקום המגורים',
            'data_sources': DATA_SOURCES,
            'responses': [
                'human_services:place:welfare_bureau',
                'human_services:care:guidance',
                'human_services:legal:advocacy_legal_aid:understand_government_programs',
            ],
            'situations': [
                'human_situations:household:families',
                'human_situations:deprivation:low_income',
            ]
        }
    }
}

FIELD_MAP = {
    'id': {
        'type': 'string',
        'transform': lambda r: f'{DATA_SOURCE_ID}:{r["semel_machlaka"]}',
    },
    # covered by airtable updater
    # 'source': {'transform': lambda r: DATA_SOURCE_ID},
    'name': {'transform': lambda r: ''},
    'phone_numbers': {
        'source': 'machlaka_phone',
        'type': 'string',
        'transform': transform_phone_numbers,
    },
    'urls': {'transform': lambda _: f'{BASE_URL}#{BRANCH_NAME_PREFIX}'},
    'email_address': {
        'source': 'email',
        'type': 'string',
        'transform': transform_email_addresses,
    },
    'address': 'adress',
    'location': {
        'source': 'address',
        'type': 'string'
    },
    'organization': {'type': 'array', 'transform': lambda r: [ORG_ID]},
    'operating_unit': {'transform': lambda _: 'מחלקת רווחה'},
    'services': {'type': 'array', 'transform': lambda r: []},
    # '__airtable_id': {}, needed to prevent error
}


def get_revaha_data():
    return fetch_datagovil_datastore('social-departments', 'המחלקות לשירותים חברתיים')


def update_services():
    extra = DF.Flow(
        DF.load(str(Path(__file__).with_name('otzma-noshmim') / 'datapackage.json')),        
    ).results()[0][0]
    extra = dict(
        ('{}:{}'.format(DATA_SOURCE_ID, x.pop('semel_machlaka')), x) for x in extra
    )

    print('update_services, extra keys {}, service map: {}'.format(list(extra.keys())[:10], list(SERVICE_MAP.keys())))

    service_ids = [x['id'] for x in SERVICES]

    def func(services, row):
        id = row['id']
        if id in extra:
            v = extra[id]
            for k, vv in v.items():
                if vv and k in SERVICE_MAP:
                    service = SERVICE_MAP[k]['id']
                    if service not in services:
                        services.append(service)
        for service in service_ids:
            if service not in services:
                services.append(service)
        return services
    
    return DF.set_type('services', transform=func, resources=['branches'])

def revaha_fetch_branch_data_flow(data=None):
    return DF.Flow(
        get_revaha_data(),
        DF.update_resource(-1, name='branches', path='branches.csv'),
        DF.rename_fields({'location': 'source_location'}, resources=['branches']),
        sort_dict_by_keys,
        *ensure_fields(FIELD_MAP, resources=['branches']),
        DF.select_fields(FIELD_MAP.keys(), resources=['branches']),
        update_services(),
        DF.add_field(
            'data',
            'object',
            lambda r: {k: v for k, v in r.items() if not k in ('id', 'source', 'status')},
            resources=['branches'],
        ),
        DF.select_fields(['id', 'data'], resources=['branches']),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/revaha'),
    )


def update_urls_from_db():
    URLS = DF.Flow(
        DF.load(str(Path(__file__).with_name('branch-urls') / 'datapackage.json')),
    ).results()[0][0]
    URLS = dict((x['code'], x['urls']) for x in URLS)

    def func(rows):
        for row in rows:
            id = row['id']
            urls = URLS.get(id)
            if urls:
                row['urls'] = urls
            else:
                if id not in URLS:
                    print(f'NO URL FOR {row.get("name")} ({id})')
            yield row
    return func


def revaha_branch_data_flow():
    return airtable_updater(
        settings.AIRTABLE_BRANCH_TABLE,
        DATA_SOURCE_ID,
        list(FIELD_MAP.keys()),
        revaha_fetch_branch_data_flow(),
        DF.Flow(
            update_mapper(),
            update_urls_from_db()
        ),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


def revaha_service_data_flow():
    services = SERVICES
    services.extend(list(SERVICE_MAP.values()))

    return airtable_updater(
        settings.AIRTABLE_SERVICE_TABLE,
        DATA_SOURCE_ID,
        list(services[0]['data'].keys()),
        services,
        update_mapper(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


def run(*_):
    logger.info('Starting Revaha Flow')
    revaha_service_data_flow()
    revaha_branch_data_flow()
    logger.info('Finished Revaha Flow')

def operator(*_):
    invoke_on(run, 'Revaha')

if __name__ == '__main__':
    operator(None, None, None)
    # DF.Flow(revaha_branch_data_flow(),DF.printer()).process()
