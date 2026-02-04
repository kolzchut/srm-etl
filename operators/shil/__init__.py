import bleach
import dataflows as DF

from conf import settings
from srm_tools.gov import get_gov_api
from srm_tools.logger import logger
from srm_tools.processors import update_mapper
from srm_tools.update_table import airtable_updater
from srm_tools.error_notifier import invoke_on

from openlocationcode import openlocationcode as olc
from pyproj import Transformer
import time

transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)

ITEM_URL_BASE = 'https://www.gov.il/he/departments/bureaus'

DATA_SOURCE_ID = 'shil'

SHIL_URL = 'https://www.gov.il/he/Departments/Guides/molsa-shill-guide'
EMERGENCY_TAG = 'human_services:internal_emergency_services'
ORG_ID = '500106406'
OPERATING_UNIT = 'תחנת שירות ייעוץ לאזרח - שי״ל'

SERVICE = {
    'id': 'shil-1',
    'data': {
        'name': 'שירות ייעוץ לאזרח',
        'source': DATA_SOURCE_ID,
        'description': '''
שי״ל הוא שירות הפועל למתן הכוונה וייעוץ לפונים בנושאי זכויות, חובות, ושירותים העומדים לרשות האזרח.
השירות ניתן בחינם ובסודיות לכל פונה, על ידי צוות עובדים ומתנדבים בתחנות שי״ל הפזורות בארץ.
השירות פועל במסגרת תחום מיצוי זכויות באגף למשאבי קהילה ועבודה קהילתית במשרד הרווחה והביטחון החברתי ומופעל בשיתוף הרשויות המקומיות.
        '''.strip(),
        'payment_required': 'no',
        'urls': '',
        # 'urls': 'https://www.gov.il/he/departments/bureaus/?OfficeId=4fa63b79-3d73-4a66-b3f5-ff385dd31cc7&categories=7cbc48b1-bf90-4136-8c16-749e77d1ecca#שירות ייעוץ לאזרח',
        'organizations': [],
        'data_sources': f'המידע התקבל מ<a target="_blank" href="{SHIL_URL}" target="_blank">האתר של שי״ל</a>',
        'responses': [
            'human_services:legal:advocacy_legal_aid',
            'human_services:legal:advocacy_legal_aid:understand_government_programs',
            EMERGENCY_TAG
        ],
        'situations': []
    },
}


def normalize_address(r):
    address = r['Address'] or {}
    city = address.get('CityDesc') or []
    street = address.get('Street') or ''
    number = address.get('HouseNumber') or 0
    if len(city) > 0 and number > 0:
        return f'{street} {number}, {city[0]}'
    elif len(city) > 0:
        return f'{street}, {city[0]}'
    else:
        return street


def get_location(r):
    address = r['Address'] or {}
    x_coord = address.get('mapiXCordinata')
    y_coord = address.get('mapiYCordinata')
    if x_coord and y_coord:
        x = int(x_coord)
        y = int(y_coord)
        lon, lat = transformer.transform(x, y)
        pc = olc.encode(lat, lon, 11)
        return pc
    return normalize_address(r)


def add_newlines(s):
    if not s:
        return ''
    for tag in ['p', 'li']:
        s = s.replace(f'</{tag}>', f'</{tag}>\n')
    return s.strip()


FIELD_MAP = {
    'id': {'source': 'ItemId', 'transform': lambda r: f'{DATA_SOURCE_ID}:{r["ItemId"]}'},
    'source': {'transform': lambda r: DATA_SOURCE_ID},
    'name': 'Title',
    'phone_numbers': {
        'source': 'PhoneNumber',
        'type': 'string',
        'transform': lambda r: '\n'.join(filter(None, [r['PhoneNumber'], r['PhoneNumber2']])),
    },
    'email_address': 'Email',
    'address_details': {
        'source': 'Location',
    },
    'description': {
        'source': 'Description',
        'transform': lambda r:bleach.clean(add_newlines(r['Description']), tags=tuple(), strip=True).replace(
            '&nbsp;', ' '
        ),
    },
    'urls': {
        'source': 'UrlName',
        'transform': lambda r: f'{ITEM_URL_BASE}/{r["UrlName"]}#{r["Title"]}',
    },
    'address': {
        'source': 'Address',
        'type': 'string',
        'transform': normalize_address,
    },
    'location': {
        'source': 'Address',
        'type': 'string',
        'transform': get_location,
    },
    'organization': {'type': 'array', 'transform': lambda r: [ORG_ID]},
    'operating_unit': {'type': 'array', 'transform': lambda r: OPERATING_UNIT},
    'services': {'type': 'array', 'transform': lambda r: [SERVICE['id']]},
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


def get_shil_data():
    skip = 0
    total, batch = get_gov_api(settings.SHIL_API, skip)
    results = list(batch)

    while len(results) < total:
        skip += len(batch)
        # Add a 1-2 second delay to avoid triggering rate limits
        time.sleep(2)
        _, batch = get_gov_api(settings.SHIL_API, skip)
        results.extend(batch)
    return results


def shil_service_data_flow():
    return airtable_updater(
        settings.AIRTABLE_SERVICE_TABLE,
        DATA_SOURCE_ID,
        list(SERVICE['data'].keys()),
        [SERVICE],
        update_mapper(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


def shil_branch_data_flow():
    airtable_updater(
        settings.AIRTABLE_BRANCH_TABLE,
        DATA_SOURCE_ID,
        list(FIELD_MAP.keys()),
        DF.Flow(
            get_shil_data(),
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


def run(*_):
    logger.info('Starting Shil Flow')

    shil_service_data_flow()
    shil_branch_data_flow()

    logger.info('Finished Shil Flow')


def operator(*_):
    invoke_on(run, 'Shil')


if __name__ == '__main__':
    operator(None, None, None)
