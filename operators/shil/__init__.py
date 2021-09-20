import bleach
import dataflows as DF
import requests
from dataflows_airtable import dump_to_airtable

from conf import settings
from srm_tools import logger

ITEM_URL_BASE = 'https://www.gov.il/he/departments/bureaus'

DATA_SOURCE_ID = 'shil'

ORGANIZATION = {
    'id': '7cbc48b1-bf90-4136-8c16-749e77d1ecca',
    'name': 'תחנות שירות ייעוץ לאזרח (שי"ל)',
    'source': DATA_SOURCE_ID,
    'kind': 'משרד ממשלתי',
    'urls': 'https://www.gov.il/he/departments/bureaus/?OfficeId=4fa63b79-3d73-4a66-b3f5-ff385dd31cc7&categories=7cbc48b1-bf90-4136-8c16-749e77d1ecca#תחנות שירות ייעוץ לאזרח',
    'description': '',
    'purpose': '',
    'status': 'ACTIVE',
}

SERVICE = {
    'id': 'shil-1',
    'name': 'שירות ייעוץ לאזרח',
    'source': DATA_SOURCE_ID,
    'description': '',
    'payment_required': 'no',
    'urls': 'https://www.gov.il/he/departments/bureaus/?OfficeId=4fa63b79-3d73-4a66-b3f5-ff385dd31cc7&categories=7cbc48b1-bf90-4136-8c16-749e77d1ecca#שירות ייעוץ לאזרח',
    'status': 'ACTIVE',
    'organizations': ['7cbc48b1-bf90-4136-8c16-749e77d1ecca'],
}


def normalize_address(r):
    _, city, _, _, street, number, *_ = r['Address'].values()
    return f'{street} {number}, {city[0]}'


FIELD_MAP = {
    'id': 'ItemId',
    'source': {'transform': lambda r: DATA_SOURCE_ID},
    'name': 'Title',
    'phone_numbers': {
        'source': 'PhoneNumber',
        'type': 'array',
        'transform': lambda r: [r['PhoneNumber'], r['PhoneNumber2']],
    },
    'address_details': {
        'source': 'Location',
    },
    'description': {
        'source': 'Description',
        'transform': lambda r: bleach.clean(r['Description'], strip=True),
    },
    'emails': {'source': 'Email', 'type': 'array', 'transform': lambda r: [r['Email']]},
    'urls': {
        'source': 'UrlName',
        'transform': lambda r: f'{ITEM_URL_BASE}/{r["UrlName"]}#{r["Title"]}',
    },
    'created_on': 'DocPublishedDate',
    'last_modified': 'DocUpdateDate',
    'address': {
        'source': 'Address',
        'type': 'string',
        'transform': normalize_address,
    },
    'location': {
        'source': 'address',
        'type': 'array',
        'transform': lambda r: [r['address']],
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
    # Note: The gov API is buggy or just weird. It looks like you can set a high limit of items,
    # but the most that get returned in any payload is 50.
    # If you pass, for example, limit 1000, the stats on the response object will say:
    # {... "total":90,"start_index":0,"page_size":1000,"current_page":1,"pages":1...}
    # but this is wrong - you only have 50 items, and it turns out you need to iterate by using
    # skip. And then, the interaction between limit and skip is weird to me.
    # you need to set a limit higher than the results we will retreive, but whatever you put in limit
    # is used, minus start_index, to declare page_size, which is wrong ......
    # we are going to batch in 50s which seems to be the upper limit for a result set.
    skip = 0
    skip_by = 50
    timeout = 5
    response = requests.get(
        settings.SHIL_API, params={'limit': 1000, 'skip': skip}, timeout=timeout
    ).json()
    total = response['total']
    results = response['results']
    while len(results) < total:
        skip += skip + skip_by
        response = requests.get(
            settings.SHIL_API, params={'limit': 1000, 'skip': skip}, timeout=timeout
        ).json()
        results.extend(response['results'])
    return results


def shil_organization_data_flow():
    return DF.Flow(
        [ORGANIZATION],
        dump_to_airtable(
            {
                (settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE): {
                    'resource-name': 'res_1',
                    'typecast': True,
                }
            }
        ),
    )


def shil_service_data_flow():
    return DF.Flow(
        [SERVICE],
        dump_to_airtable(
            {
                (settings.AIRTABLE_BASE, settings.AIRTABLE_SERVICE_TABLE): {
                    'resource-name': 'res_1',
                    'typecast': True,
                }
            }
        ),
    )


def shil_branch_data_flow():
    return DF.Flow(
        get_shil_data(),
        DF.update_package(name='Shil Scrape', resources=-1),
        DF.update_resource(name='branches', path='branches.csv', resources=-1),
        *[ensure_field(key, args) for key, args in FIELD_MAP.items()],
        DF.select_fields(list(FIELD_MAP.keys())),
        dump_to_airtable(
            {
                (settings.AIRTABLE_BASE, settings.AIRTABLE_BRANCH_TABLE): {
                    'resource-name': 'branches',
                    'typecast': True,
                }
            }
        ),
    )


def operator(*_):
    logger.info('Starting Shil Flow')

    shil_organization_data_flow().process()
    shil_service_data_flow().process()
    shil_branch_data_flow().process()

    logger.info('Finished Shil Flow')


if __name__ == '__main__':
    operator(None, None, None)
