import hashlib
import re

import dataflows as DF
import requests
import slugify

from conf import settings
from srm_tools.logger import logger
from srm_tools.processors import ensure_fields, update_mapper
from srm_tools.update_table import airtable_updater


def transform_phone_numbers(r):
    phone_numbers = r['authority_phone'] or ''
    machlaka_phone = r['machlaka_phone'] or ''
    if machlaka_phone:
        phone_numbers = f'{phone_numbers},{machlaka_phone}'
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
            slugify.slugify(str(v if v is not None else '').strip(), lowercase=True, separator='')
            for v in row.values()
        ]
    ).encode('utf-8')
    sha = hashlib.sha1()
    sha.update(input)
    return f'{DATA_SOURCE_ID}:{sha.hexdigest()}'


BASE_URL = (
    'https://www.gov.il/he/departments/dynamiccollectors/molsa-social-departmentsd-list?skip=0'
)

DATA_SOURCE_ID = 'revaha'

BRANCH_NAME_PREFIX = 'מחלקה לשירותים חברתיים'

ORGANIZATION = {
    # the id is just a uuid I generated
    'id': '53a2e790-87b3-44a2-a5f2-5b826f714775',
    'data': {
        'name': 'משרד הרווחה והביטחון החברתי',
        'source': DATA_SOURCE_ID,
        'kind': 'משרד ממשלתי',
        'urls': f'{BASE_URL}#{BRANCH_NAME_PREFIX}',
        'description': '',
        'purpose': '',
        'status': 'ACTIVE',
    },
}

SERVICE = {
    'id': '',
    'data': {},
}

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
        'type': 'array',
        'transform': lambda r: [r['adress']],
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


def revaha_organization_data_flow():
    return airtable_updater(
        settings.AIRTABLE_ORGANIZATION_TABLE,
        DATA_SOURCE_ID,
        list(ORGANIZATION['data'].keys()),
        [ORGANIZATION],
        update_mapper(),
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


def revaha_branch_data_flow():
    return airtable_updater(
        settings.AIRTABLE_BRANCH_TABLE,
        DATA_SOURCE_ID,
        list(FIELD_MAP.keys()),
        revaha_fetch_branch_data_flow(),
        update_mapper(),
    )


def operator(*_):
    logger.info('Starting Revaha Flow')
    revaha_organization_data_flow()
    revaha_branch_data_flow()
    logger.info('Finished Revaha Flow')


if __name__ == '__main__':
    # operator(None, None, None)
    DF.Flow(revaha_branch_data_flow(),DF.printer()).process()
