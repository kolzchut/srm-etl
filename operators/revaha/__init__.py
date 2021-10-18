import hashlib
import re

import dataflows as DF
import requests
import slugify

from conf import settings
from srm_tools.logger import logger
from srm_tools.processors import ensure_fields
from srm_tools.update_table import airflow_table_updater


def transform_phone_numbers(r):
    phone_numbers = r['authority_phone'] if r['authority_phone'] else ''
    machlaka_phone = r['machlaka_phone'] if r['machlaka_phone'] else ''
    if machlaka_phone:
        phone_numbers += machlaka_phone
    return phone_numbers.replace(' ', '')


def transform_email_addresses(r):
    pattern = r'[\w.+-]+@[\w-]+\.[\w.-]+'
    match = re.search(pattern, r['email']) if r['email'] else None
    return match.group(0) if match else None


def sort_dict_by_keys(row):
    return dict(sorted(row.items(), key=lambda i: i[0]))


def make_unique_id_from_values(row):
    input = ''.join(
        [
            slugify.slugify(str(v if v is not None else '').strip().lower(), space_replacement="")
            for v in row.values()
        ]
    ).encode('utf-8')
    sha = hashlib.sha1()
    sha.update(input)
    return f'{DATA_SOURCE_ID}:{sha}'


DATA_SOURCE_ID = 'revaha'

BRANCH_NAME = ''

BRANCH_DESCRIPTION = ''

ORGANIZATION = {'id': '', 'name': 'משרד הרווחה והביטחון החברתי'}

SERVICE = {'id': ''}

FIELD_MAP = {
    'id': 'id',
    'source': {'transform': lambda r: DATA_SOURCE_ID},
    'name': {'transform': lambda r: DATA_SOURCE_ID},
    'phone_numbers': {
        'source': 'machlaka_phone',
        'type': 'string',
        'transform': transform_phone_numbers,
    },
    'emails': {'source': 'email', 'type': 'array', 'transform': transform_email_addresses},
    'address': 'adress',
    'location': {
        'source': 'address',
        'type': 'array',
        'transform': lambda r: [r['adress']],
    },
    'organization': {'type': 'array', 'transform': lambda r: [ORGANIZATION['id']]},
    'services': {'type': 'array', 'transform': lambda r: [SERVICE['id']]},
}


def gov_data_proxy(template_id, skip):
    data = {
        'DynamicTemplateID': template_id,
        'QueryFilters': {'skip': {'Query': skip}},
        'From': skip,
    }
    timeout = 30
    response = requests.post(
        settings.GOV_DATA_PROXY,
        json=data,
        timeout=timeout,
    ).json()
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
        _, batch = gov_data_proxy(template_id, skip)
        results.extend(batch)
    return results


def revaha_branch_data_flow():
    return DF.Flow(
        (obj['Data'] for obj in get_revaha_data()),
        sort_dict_by_keys,
        DF.add_field('id', 'string', make_unique_id_from_values),
        *ensure_fields(FIELD_MAP),
        DF.printer(),
    )


def operator(*_):
    logger.info('Starting Revaha Flow')
    revaha_branch_data_flow().process()
    logger.info('Finished Revaha Flow')


if __name__ == '__main__':
    operator(None, None, None)
