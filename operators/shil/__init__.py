import bleach
import dataflows as DF

from conf import settings
from srm_tools.gov import get_gov_api
from srm_tools.logger import logger
from srm_tools.processors import update_mapper
from srm_tools.update_table import airtable_updater

ITEM_URL_BASE = 'https://www.gov.il/he/departments/bureaus'

DATA_SOURCE_ID = 'shil'

SHIL_URL = 'https://www.gov.il/he/Departments/Guides/molsa-shill-guide'

ORGANIZATION = {
    'id': '7cbc48b1-bf90-4136-8c16-749e77d1ecca',
    'data': {
        'name': 'תחנות שירות ייעוץ לאזרח (שי"ל)',
        'source': DATA_SOURCE_ID,
        'kind': 'משרד ממשלתי',
        'urls': SHIL_URL,
        'phone_numbers': '118',
        # 'urls': 'https://www.gov.il/he/departments/bureaus/?OfficeId=4fa63b79-3d73-4a66-b3f5-ff385dd31cc7&categories=7cbc48b1-bf90-4136-8c16-749e77d1ecca#תחנות שירות ייעוץ לאזרח',
        'description': '',
        'purpose': '',
    },
}

SERVICE = {
    'id': 'shil-1',
    'data': {
        'name': 'שירות ייעוץ לאזרח',
        'source': DATA_SOURCE_ID,
        'description': '',
        'payment_required': 'no',
        'urls': '',
        # 'urls': 'https://www.gov.il/he/departments/bureaus/?OfficeId=4fa63b79-3d73-4a66-b3f5-ff385dd31cc7&categories=7cbc48b1-bf90-4136-8c16-749e77d1ecca#שירות ייעוץ לאזרח',
        'organizations': ['7cbc48b1-bf90-4136-8c16-749e77d1ecca'],
        'data_sources': f'המידע התקבל מ<a target="_blank" href="{SHIL_URL}" target="_blank">האתר של שי״ל</a>'
    },
}


def normalize_address(r):
    _, city, _, _, street, number, *_ = r['Address'].values()
    return f'{street} {number}, {city[0]}'


def add_newlines(s):
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
    'email_addresses': 'Email',
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
        'source': 'address',
        'type': 'string',
    },
    'organization': {'type': 'array', 'transform': lambda r: [ORGANIZATION['id']]},
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
    skip_by = 50
    total, results = get_gov_api(settings.SHIL_API, skip)

    while len(results) < total:
        skip += skip + skip_by
        _, batch = get_gov_api(settings.SHIL_API, skip)
        results.extend(batch)
    return results


def shil_organization_data_flow():
    airtable_updater(
        settings.AIRTABLE_ORGANIZATION_TABLE,
        DATA_SOURCE_ID,
        list(ORGANIZATION['data'].keys()),
        [ORGANIZATION],
        update_mapper(),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )


def shil_service_data_flow():
    return airtable_updater(
        settings.AIRTABLE_SERVICE_TABLE,
        DATA_SOURCE_ID,
        list(SERVICE['data'].keys()),
        [SERVICE],
        update_mapper(),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
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
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )


def operator(*_):
    logger.info('Starting Shil Flow')

    shil_organization_data_flow()
    shil_service_data_flow()
    shil_branch_data_flow()

    logger.info('Finished Shil Flow')


if __name__ == '__main__':
    operator(None, None, None)
