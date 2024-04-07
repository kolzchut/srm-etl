import bleach
import dataflows as DF
import shutil

from conf import settings
from srm_tools.gov import get_gov_api
from srm_tools.logger import logger
from srm_tools.processors import update_mapper
from srm_tools.update_table import airtable_updater

from openlocationcode import openlocationcode as olc

ITEM_URL_BASE = 'https://tipatchalavappointments.health.gov.il/patient-details?codeStation='

DATA_SOURCE_ID = 'tipat-halav'

TIPAT_URL = 'https://healthinstitutionsapi.health.gov.il/api/TipotChalav/GetTipotChalavResult?maxResults=1000&city=&instituteExtended=&ownerShip=&district=&alternative=false&instCode=&xCordinate=0&yCordinate=0'
TIPAT_WEBSITE = 'https://healthinstitutions.health.gov.il/TipotChalav'

CHECKPOINT = 'tipat-halav'

ORGS = {
    'הסהר האדום': '580205615',
    'משרד הבריאות': '500100904',
    'שירותי בריאות כללית': 'srm0012', 
    'מכבי שירותי בריאות': 'srm0011', 
    'קופת חולים מאוחדת': 'srm0013',
    'קופת חולים לאומית': 'srm0010',
    'עיריית ירושלים': '500230008',
    'עיריית תל אביב יפו': '500250006',
}

SERVICE_BASE = {
    'data_sources': f'המידע התקבל מ<a target="_blank" href="{TIPAT_WEBSITE}" target="_blank">אתר משרד הבריאות</a>',
    'responses': [
        'human_services:place:health:clinic:well_baby_clinic',
        'human_services:health:prevent_treat:vaccinations',
        'human_services:care:support_group:parenting_education',
        'human_services:food:nutrition',
        'human_services:health:prevent_treat:checkup_test:disease_screening',
    ],
    'situations': [
        'human_situations:household:families:fathers',
        'human_situations:household:families:mothers',
        'human_situations:household:families:parents',
        'human_situations:age_group:infants',
    ]
}

SERVICE_LOCAL = {
    'id': 'tipat-halav-1',
    'data': {
        'name': 'טיפת חלב',
        # 'source': DATA_SOURCE_ID,
        'description': 'טיפת חלב היא תחנה המספקת שירותי בריאות ורפואה בתחום קידום בריאות ומניעה לנשים הרות, תינוקות וילדים (גילאי לידה עד 6 שנים) ומשפחותיהם. תחנות טיפת חלב פזורות בכל רחבי הארץ ומופעלות על-ידי לשכות הבריאות. טיפת חלב היא  השירות הראשון שפוגשות משפחות צעירות, אשר מלווה אותן, במקצועיות, במגוון נושאים, החל משלב ההיריון, ההכנה ללידה וההורות',
        'payment_required': 'no',
        'phone_numbers': None,
        'urls': None,
        **SERVICE_BASE,
    },
}
SERVICE_NATIONAL = {
    'id': 'tipat-halav-2',
    'data': {
        'name': 'טיפת חלב - מוקד טלפוני',
        # 'source': DATA_SOURCE_ID,
        'description': 'מוקד טיפת חלב בטלפון מאויש על ידי אחיות טיפת חלב, יועצות הנקה, יועצות שינה ותזונאיות מומחיות לגיל הרך.\n\nהייעוץ ניתן להורים לילדים מגיל לידה עד גיל 6, ללא קשר לטיפת החלב שבה מטופל הילד או הילדה, בנוסף לשירות הניתן בטיפות חלב בשעות הפעילות הרגילות. משפחות המקבלות טיפול בטיפות חלב של משרד הבריאות יקבלו במוקד הטלפוני מענה המבוסס על המידע הקליני המצוי ברשומה הרפואית שלהן, לאחר הזדהות. ההמלצות האישיות יתועדו כדי לשמור על הרצף הטיפולי בטיפת החלב אליה שייכים הפונים. משפחות המקבלות טיפול בטיפות חלב של קופות החולים מכבי, כללית, מאוחדת ולאומית יקבלו במוקד הטלפוני מענה המבוסס על המלצות והנחיות משרד הבריאות.\n\nבין הנושאים שבתחום אחריות טיפות החלב ושניתן לקבל בהם ייעוץ מהמוקד: תופעות לוואי לאחר חיסונים, התפתחות, הנקה, תזונה, בכי, בעיות בשינה.',
        'payment_required': 'no',
        'urls': TIPAT_WEBSITE,
        'phone_numbers': '*5400',
        **SERVICE_BASE,
    }
}

NATIONAL_BRANCH = {
    'code': 'national',
    'stationName': 'טיפת חלב - מוקד טלפוני',
    'ownerShip': 'משרד הבריאות',
    'remarks': None,
    'addressComments': None,
    'phone_numbers': None,
    'email': None,
}

def normalize_address(r):
    if r['code'] == 'national':
        return 'שירות ארצי'
    district = r.get('district') or ''
    city = r.get('cityName') or ''
    street = r.get('streetName') or ''
    number = r.get('buildingNum') or ''
    if city and street and number:
        return f'{street} {number}, {city}'
    elif city and street:
        return f'{street}, {city}'
    elif city:
        return city
    else:
        return district
    

def get_location(r):
    x_coord = r.get('xCordinate')
    y_coord = r.get('yCordinate')
    if x_coord and y_coord:
        pc = olc.encode(y_coord, x_coord, 11)
        return pc
    return normalize_address(r)


FIELD_MAP = {
    'id': {'source': 'code', 'transform': lambda r: f'{DATA_SOURCE_ID}:{r["code"]}'},
    # 'source': {'transform': lambda r: DATA_SOURCE_ID},
    'name': 'stationName',
    'organization': {'type': 'array', 'transform': lambda r: [ORGS[r['ownerShip']]]},
    'services': {'type': 'array', 'transform': lambda r: [SERVICE_LOCAL['id'] if r['code'] != 'national' else SERVICE_NATIONAL['id']]},
    'description': 'remarks',
    'address_details': 'addressComments',
    'phone_numbers': 'phone1',
    'email_address': 'email',
    'address': {'transform': normalize_address},
    'location': {'transform': get_location},
    'urls': {'transform': lambda r: f'{ITEM_URL_BASE}{r["code"]}' if r['code'] != 'national' else None},
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
    skip_by = 50
    total, results = get_gov_api(settings.SHIL_API, skip)

    while len(results) < total:
        skip += skip + skip_by
        _, batch = get_gov_api(settings.SHIL_API, skip)
        results.extend(batch)
    return results


def tipat_service_data_flow():
    return airtable_updater(
        settings.AIRTABLE_SERVICE_TABLE,
        DATA_SOURCE_ID,
        list(SERVICE_NATIONAL['data'].keys()),
        [SERVICE_LOCAL, SERVICE_NATIONAL],
        update_mapper(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )


def tipat_branch_data_flow(data):
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

def get_tipat_data():
    return DF.Flow(
        DF.load(TIPAT_URL, format='json'),
        DF.checkpoint(CHECKPOINT),
        DF.set_type('code', type='string', transform=str),
        DF.filter_rows(lambda r: r.get('status') == 'פעיל'),
        DF.printer()
    ).results()[0][0] + [NATIONAL_BRANCH]


def operator(*_):
    logger.info('Starting Shil Flow')

    shutil.rmtree(f'.checkpoints/{CHECKPOINT}', ignore_errors=True, onerror=None)

    tipat_service_data_flow()
    DATA = get_tipat_data()
    tipat_branch_data_flow(DATA)

    logger.info('Finished Shil Flow')


if __name__ == '__main__':
    operator(None, None, None)
