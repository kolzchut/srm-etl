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
from srm_tools.logger import logger
from srm_tools.processors import ensure_fields, update_mapper
from srm_tools.update_table import airtable_updater
from srm_tools.scraping_utils import overcome_blocking


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


def make_unique_id_from_values(row):
    keys = sorted(row.keys())
    values = [row[k] for k in keys]
    input = ''.join(
        [
            slugify.slugify(str(v if v is not None else '').strip(), lowercase=True, separator='')
            for v in values
        ]
    ).encode('utf-8')
    sha = hashlib.sha1()
    sha.update(input)
    return f'{DATA_SOURCE_ID}:{sha.hexdigest()}'


BASE_URL = (
    'https://www.gov.il/he/departments/dynamiccollectors/molsa-social-departmentsd-list?skip=0'
)

DATA_SOURCE_ID = 'revaha'
DATA_SOURCES = f'המידע מ<a target="_blank" href="{BASE_URL}" target="_blank">אתר משרד הרווחה</a>'
PAYMENT_DETAILS = 'נדרש תיאום מראש'

BRANCH_NAME_PREFIX = 'מחלקה לשירותים חברתיים'

ORGANIZATION = {
    # the id is just a uuid I generated
    'id': 'srm0020',
    'data': {
        'name': 'משרד הרווחה והביטחון החברתי',
        'source': DATA_SOURCE_ID,
        'kind': 'משרד ממשלתי',
        'phone_numbers': '118',
        'urls': f'{BASE_URL}#{BRANCH_NAME_PREFIX}',
        'description': '',
        'purpose': '',
    },
}

session = requests.Session()

SERVICES = [
    {
        'id': 'revaha-seniors',
        'data': {
            'name': 'סיוע לאזרחים ותיקים מטעם המחלקה לשירותים חברתיים',
            'source': DATA_SOURCE_ID,
            'description': 'השירות לאוכלוסיית התושבים הוותיקים ובני משפחותיהם ניתן במחלקות לשירותים חברתיים וכולל מיצוי זכויות, מידע על מסגרות יומיות ,שירותי סעד, עובדים זרים, דיור מוגן ומסגרות מוסדיות זמניות וקבועות.',
            'payment_required': 'no',
            'urls': '',
            'organizations': ['srm0020'],
            'payment_details': PAYMENT_DETAILS,
            'data_sources': DATA_SOURCES
        },
    },
    {
        'id': 'revaha-disabilities',
        'data': {
            'name': 'סיוע לאנשים עם מוגבלות מטעם המחלקה לשירותים חברתיים',
            'source': DATA_SOURCE_ID,
            'description': 'השירות לאנשים  עם מוגבלות ובני משפחותיהם ניתן במחלקות לשירותים חברתיים ומיועד לאנשים עם פיגור שכלי, אוטיזם, מוגבלויות פיזיות וחושיות (עיוורון וחירשות) ולבני משפחותיהם. למימוש הזכאות לשירותים יש צורך בהכרה של משרד הרווחה והשירותים החברתיים.',
            'payment_required': 'no',
            'urls': '',
            'organizations': ['srm0020'],
            'payment_details': PAYMENT_DETAILS,
            'data_sources': DATA_SOURCES
        },
    },
    {
        'id': 'revaha-aid',
        'data': {
            'name': 'תמיכה וייעוץ ליחידים ומשפחות מטעם המחלקה לשירותים חברתיים',
            'source': DATA_SOURCE_ID,
            'description': 'השירות מסייע לילדים, בני נוער, משפחות, יחידים, מוגבלים, זקנים, עולים חדשים ולכל פרט/קבוצה החפצים בקבלת סיוע. המחלקות לשירותים חברתיים  מעניקות מידע, יעוץ, טיפול, שירותים סוציאליים, הכוונה, תיווך לקבלת שירות, שילוב במסגרות ושירותי עזר בבית - בהתאם לכללי נזקקות וזכאות ולאפשרויות התקציביות.',
            'payment_required': 'no',
            'urls': '',
            'organizations': ['srm0020'],
            'payment_details': PAYMENT_DETAILS,
            'data_sources': DATA_SOURCES
        },
    },
]

FIELD_MAP = {
    'id': 'id',
    # covered by airtable updater
    # 'source': {'transform': lambda r: DATA_SOURCE_ID},
    'name': {'transform': lambda r: f'{BRANCH_NAME_PREFIX} {r["source_location"]}'},
    'phone_numbers': {
        'source': 'machlaka_phone',
        'type': 'string',
        'transform': transform_phone_numbers,
    },
    'urls': f'{BASE_URL}#{BRANCH_NAME_PREFIX}',
    'email_addresses': {
        'source': 'email',
        'type': 'string',
        'transform': transform_email_addresses,
    },
    'address': 'adress',
    'location': {
        'source': 'address',
        'type': 'string'
    },
    'organization': {'type': 'array', 'transform': lambda r: [ORGANIZATION['id']]},
    'services': {'type': 'array', 'transform': lambda r: []},
    # '__airtable_id': {}, needed to prevent error
}


def gov_data_proxy(template_id, skip):
    data = {
        'DynamicTemplateID': template_id,
        'QueryFilters': {'skip': {'Query': skip}},
        'From': skip,
    }
    timeout = 30
    resp = overcome_blocking(
        session,
        lambda: session.post(
            settings.GOV_DATA_PROXY,
            json=data,
            timeout=timeout,
        )
    )
    response = resp.json()
    total, results = response['TotalResults'], response['Results']

    return total, results


def get_revaha_data():
    skip = 0
    # seems to only ever return 10 results in a call
    skip_by = 10
    template_id = '23ede39d-968c-4e5c-8098-9c58b037a0c3'
    total, results = gov_data_proxy(template_id, skip)

    while len(results) < total:
        skip += skip_by
        for _ in range(3):
            _, batch = gov_data_proxy(template_id, skip)
            if len(batch) > 0:
                results.extend(batch)
                print(f'SKIPPED {skip}, GOT {len(results)}, TOTAL {total}')
                break
            time.sleep(10)
        else:
            break
    print('FETCHED {} REVAHA RECORDS'.format(len(results)))
    assert len(results) > 0
    return results


def revaha_organization_data_flow():
    return airtable_updater(
        settings.AIRTABLE_ORGANIZATION_TABLE,
        DATA_SOURCE_ID,
        list(ORGANIZATION['data'].keys()),
        [ORGANIZATION],
        update_mapper(),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )


def revaha_fetch_branch_data_flow(data=None):
    return DF.Flow(
        (obj['Data'] for obj in data or get_revaha_data()),
        DF.update_resource(-1, name='branches', path='branches.csv'),
        DF.rename_fields({'location': 'source_location'}, resources=['branches']),
        sort_dict_by_keys,
        DF.add_field('id', 'string', make_unique_id_from_values, resources=['branches']),
        *ensure_fields(FIELD_MAP, resources=['branches']),
        DF.select_fields(FIELD_MAP.keys(), resources=['branches']),
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
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )


def revaha_service_data_flow():
    return airtable_updater(
        settings.AIRTABLE_SERVICE_TABLE,
        DATA_SOURCE_ID,
        list(SERVICES[0]['data'].keys()),
        SERVICES,
        update_mapper(),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )


def operator(*_):
    logger.info('Starting Revaha Flow')
    revaha_organization_data_flow()
    revaha_service_data_flow()
    revaha_branch_data_flow()
    logger.info('Finished Revaha Flow')


if __name__ == '__main__':
    operator(None, None, None)
    # DF.Flow(revaha_branch_data_flow(),DF.printer()).process()
