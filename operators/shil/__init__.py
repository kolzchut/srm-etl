import bleach
import dataflows as DF
import requests

from conf import settings
from srm_tools import logger

ITEM_URL_BASE = 'https://www.gov.il/he/departments/bureaus'


def normalize_address(r):
    _, city, _, _, street, number, *_ = r['Address'].values()
    return f'{street} {number}, {city[0]}'


FIELD_MAP = {
    'uuid': 'ItemUniqueId',
    'id': 'ItemId',
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
    # # TODO - we should be saving branch emails no?
    'emails': {'source': 'Email', 'type': 'array', 'transform': lambda r: [r['Email']]},
    # # TODO - Notes?
    # # TODO - How do we persist units - or where is it in our data model?
    'urls': {
        'source': 'UrlName',
        'transform': lambda r: f'{ITEM_URL_BASE}/{r["UrlName"]}',
    },
    'created_on': 'DocPublishedDate',
    'last_modified': 'DocUpdateDate',
    'address': {
        'source': 'Address',
        'type': 'string',
        'transform': normalize_address,
    },
}


def ensure_field(name, args):
    args = {'source': args} if isinstance(args, str) else args
    name, type, transform = (
        name,
        args.get('type', 'string'),
        args.get('transform', lambda r: r[args['source']]),
    )
    return DF.add_field(name, type, transform)


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
    response = requests.get(
        settings.SHIL_API, params={'limit': 1000, 'skip': 0}, timeout=10
    ).json()
    skip = 0
    total = response['total']
    results = response['results']
    while len(results) < total:
        response = requests.get(
            settings.SHIL_API,
            params={'limit': 1000, 'skip': skip + 50},
            timeout=10,
        ).json()
        results.extend(response['results'])
    return results


def shil_data_flow():
    return DF.Flow(
        get_shil_data(),
        *[ensure_field(key, args) for key, args in FIELD_MAP.items()],
        DF.select_fields(list(FIELD_MAP.keys())),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/shil'),
    )


def operator(*_):
    logger.info('Starting Shil Flow')

    shil_data_flow().process()

    logger.info('Finished Shil Flow')


if __name__ == '__main__':
    operator(None, None, None)
