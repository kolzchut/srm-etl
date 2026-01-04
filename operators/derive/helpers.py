from collections import Counter
import dataflows as DF
from dataflows_airtable import AIRTABLE_ID_FIELD
from dataflows.helpers.resource_matcher import ResourceMatcher
import re
import regex

from srm_tools.stats import Stats

ACCURATE_TYPES = ('ROOFTOP', 'RANGE_INTERPOLATED', 'STREET_MID_POINT', 'ADDR_V1', 'ADDRESS_POINT', 'ADDRESS') #'POI_MID_POINT')
DIGIT = re.compile(r'\d')
ENGLISH = re.compile(r'[a-zA-Z+]')

__stats: Stats = None

def get_stats():
    global __stats
    if not __stats:
        __stats = Stats()
    return __stats


def transform_urls(urls):
    def transformer(s):
        href, *title = s.rsplit('#', 1)
        title = title[0] if title else 'קישור'
        return {'href': href, 'title': title}

    return list(map(transformer, urls.split('\n'))) if urls else None


def transform_phone_numbers(phone_numbers):
    phone_numbers = phone_numbers.split('\n') if phone_numbers else []
    ret = []
    for number in phone_numbers:
        number = number.strip()
        digits = ''.join(DIGIT.findall(number))
        if len(digits) > 10 and digits.startswith('972'):
            digits = digits[3:]
            if len(digits) < 10 and not digits.startswith('0'):
                digits = '0' + digits
        if len(digits) == 9 and digits.startswith('0'):
            digits = [digits[0:2], digits[2:5], digits[5:]]
        elif len(digits) == 10 and digits.startswith('0'):
            digits = [digits[0:3], digits[3:6], digits[6:]]
        elif len(digits) == 10 and digits.startswith('1'):
            digits = [digits[:1], digits[1:4], digits[4:]]
        else:
            digits = None
        if digits:
            number = '-'.join(digits)
        if number:
            ret.append(number)
    return ret


def calc_point_id(geometry):
    return ''.join('{:08.5f}'.format(c) for c in geometry).replace('.','')


def calculate_branch_short_name(row):
    on = row['organization_name']
    osn = row['organization_short_name']
    if osn:
        return f'{osn}'
    return on


def validate_geometry(geometry):
    if geometry:
        if len(geometry) == 2:
            # Y: 29.3-33.3
            # X: 33-37
            if 33 < geometry[0] < 37 and 29.3 < geometry[1] < 33.3:
                return True
    return False


def validate_address(address):
    if address:
        if len(ENGLISH.findall(address)) == 0:
            return True        
    return False

def filter_dummy_data():
    return DF.filter_rows(lambda r: not any([r.get('id') == 'dummy', r.get('name') == 'dummy']))


def filter_active_data(resource, statName):
    return get_stats().filter_with_stat(statName, lambda r: r.get('status') != 'INACTIVE', resources=resource)


def set_staging_pkey(resource_name):
    return DF.rename_fields({AIRTABLE_ID_FIELD: 'key'}, resources=[resource_name])


def update_taxonomy_with_parents(v):
    ids = v or []
    ret = set()
    for id in ids:
        parts = id.split(':')
        for i in range(2, len(parts) + 1):
            ret.add(':'.join(parts[:i]))
    return sorted(ret)


def reorder_responses_by_category(responses, category):
    return (
        [r for r in responses if r['id'].split(':')[1] == category] +
        [r for r in responses if r['id'].split(':')[1] != category]
    )


def reorder_records_by_category(records, category):
    return (
        [r for r in records if r['response_category'] == category] +
        [r for r in records if r['response_category'] != category]
    )


def preprocess_responses(select_fields=None, validate=False):
    return DF.Flow(
        DF.update_resource(['Responses'], name='responses', path='responses.csv'),
        filter_dummy_data(),
        filter_active_data('responses', 'Processing: Responses: Inactive'),
        set_staging_pkey('responses'),
        DF.set_type('synonyms', type='array', transform=lambda v: tuple(v.strip().split('\n')) if v else tuple(), resources=['responses']),
        DF.select_fields(select_fields, resources=['responses']) if select_fields else None,
        DF.validate() if validate else None,
    )


def preprocess_situations(select_fields=None, validate=False):
    return DF.Flow(
        DF.update_resource(['Situations'], name='situations', path='situations.csv'),
        filter_dummy_data(),
        filter_active_data('situations', 'Processing: Situations: Inactive'),
        set_staging_pkey('situations'),
        DF.set_type('synonyms', type='array', transform=lambda v: tuple(v.strip().split('\n')) if v else tuple(), resources=['situations']),
        DF.select_fields(select_fields, resources=['situations']) if select_fields else None,
        DF.validate() if validate else None,
    )


def preprocess_services(select_fields=None, validate=False):
    return DF.Flow(
        DF.update_resource(['Services'], name='services', path='services.csv'),
        filter_dummy_data(),
        filter_active_data('services', 'Processing: Services: Inactive'),
        set_staging_pkey('services'),
        DF.set_type('urls', type='array', transform=transform_urls, resources=['services']),
        DF.set_type('name', transform=lambda v, row: row['name_manual'] or v, resources=['services']),
        DF.set_type('situation_ids', transform=lambda _, row: row['situations_manual_ids'] or row['situation_ids'], resources=['services']),
        DF.set_type('response_ids', transform=lambda _, row: row['responses_manual_ids'] or row['response_ids'], resources=['services']),
        DF.set_type('boost', type='number', transform=lambda v: v or 0, resources=['services']),
        DF.set_type(
            'phone_numbers',
            type='array',
            transform=transform_phone_numbers,
            resources=['services'],
        ),
        DF.set_type(
            'data_sources',
            type='array',
            transform=lambda v: v.split('\n') if v else [],
            resources=['services'],
        ),
        DF.delete_fields(['situations_manual', 'responses_manual', 'name_manual', 'responses_manual_ids'], resources=['services']),
        DF.select_fields(select_fields, resources=['services']) if select_fields else None,
        DF.validate() if validate else None,
    )


def preprocess_organizations(select_fields=None, validate=False):
    return DF.Flow(
        DF.update_resource(['Organizations'], name='organizations', path='organizations.csv'),
        filter_dummy_data(),
        filter_active_data('organizations', 'Processing: Organizations: Inactive'),
        get_stats().filter_with_stat('Processing: Organizations: No Name', lambda r: bool(r.get('name')), resources=['organizations']),
        set_staging_pkey('organizations'),
        DF.set_type('urls', type='array', transform=transform_urls, resources=['organizations']),
        DF.set_type(
            'phone_numbers',
            type='array',
            transform=transform_phone_numbers,
            resources=['organizations'],
        ),
        remove_whitespaces('organizations', 'name'),
        remove_whitespaces('organizations', 'short_name'),
        DF.select_fields(select_fields, resources=['organizations']) if select_fields else None,
        DF.validate() if validate else None,
    )


def preprocess_branches(validate=False):
    select_fields = [
        'key', 'id', 'source', 'status', 'name', 'organization', 'operating_unit', 'location', 'address', 'address_details', 'description', 'phone_numbers', 'email_address', 'urls', 'manual_url', 'fixes', 'situations', 'services'
    ]
    return DF.Flow(
        DF.update_resource(['Branches'], name='branches', path='branches.csv'),
        filter_dummy_data(),
        filter_active_data('branches', 'Processing: Branches: Inactive'),
        set_staging_pkey('branches'),
        DF.select_fields(select_fields, resources=['branches']),
        DF.set_type('urls', type='array', transform=transform_urls, resources=['branches']),
        DF.set_type(
            'phone_numbers',
            type='array',
            transform=transform_phone_numbers,
            resources=['branches'],
        ),
        remove_whitespaces('branches', 'name'),
        DF.validate() if validate else None,
    )


def preprocess_locations(validate=False):
    select_fields = [
        'key', 'id', 'status', 'provider', 'accuracy', 'alternate_address', 'resolved_lat', 'resolved_lon', 'resolved_address', 'resolved_city', 'fixed_lat', 'fixed_lon'
    ]
    return DF.Flow(
        DF.update_resource(['Locations'], name='locations', path='locations.csv'),
        filter_dummy_data(),
        set_staging_pkey('locations'),
        DF.select_fields(select_fields, resources=['locations']),
        DF.add_field(
            'national_service', 'boolean',
            lambda r: r['accuracy'] == 'NATIONAL_SERVICE',
            resources=['locations'],
        ),
        get_stats().filter_with_stat('Processing: Locations: No Location',
            lambda r: any(
                all(r.get(f) for f in fields)
                for fields in [('resolved_lat', 'resolved_lon'), ('fixed_lat', 'fixed_lon'), ('national_service',)]
            ),
            resources=['locations'],
        ),
        DF.add_field(
            'location_accurate', 'boolean',
            lambda r: (r['accuracy'] in ACCURATE_TYPES) or all((r.get('fixed_lat'), r['fixed_lon'])) or False,
            resources=['locations'],
        ),
        get_stats().filter_with_stat('Processing: Locations: No Lat', lambda r: any(r[x] for x in ('fixed_lat', 'resolved_lat', 'national_service')), resources=['locations']),
        get_stats().filter_with_stat('Processing: Locations: No Lon', lambda r: any(r[x] for x in ('fixed_lon', 'resolved_lon', 'national_service')), resources=['locations']),
        DF.add_field(
            'lat',
            'number',
            lambda r: r.get('fixed_lat') or r['resolved_lat'],
            resources=['locations'],
        ),
        DF.add_field(
            'lon',
            'number',
            lambda r: r.get('fixed_lon') or r['resolved_lon'],
            resources=['locations'],
        ),
        DF.add_field(
            'geometry', 'geopoint', lambda r: [r['lon'], r['lat']] if not r['national_service'] else None, resources=['locations']
        ),
        DF.add_field(
            'address',
            'string',
            lambda r: r.get('resolved_address') or r['id'],
            resources=['locations'],
        ),
    )


def point_offset_table():
    """Lookup table for positioning up to seven points."""
    # https://github.com/kolzchut/srm-etl/issues/8
    from math import cos, pi, sin

    diameters = [(d / 2 - 0.5) for d in [2, 2.15470, 2.41421, 2.70130, 3.00000]]
    first = [[1, [[0.0, 0.0]]]]
    generated = [
        [
            n,
            [
                [round(d * sin(i / n * 2 * pi), 3), -round(d * cos(i / n * 2 * pi), 3)]
                for i in range(n)
            ],
        ]
        for n, d in zip([2, 3, 4, 5, 6], diameters)
    ]
    last = [[7, [[0.0, 0.0]] + generated[4][1]]]
    return first + generated + last


POINT_OFFSETS = dict(point_offset_table())


def generate_offset(item_key, siblings_key):
    def func(r):
        count = len(r[siblings_key])
        index = r[siblings_key].index(r[item_key]) + 1
        offset = f'{count}-{index}' if count in POINT_OFFSETS.keys() else None
        return offset

    return func


def most_common_category(row):
    response_categories = row['response_categories']
    if len(response_categories) == 0:
        # print('ERROR: no response categories', repr(row))
        return None
    return Counter(response_categories).most_common(1)[0][0]


def address_parts(row):
    resolved_address: str = row['branch_address']
    orig_address: str = row['branch_orig_address']
    accurate: bool = row['branch_location_accurate']
    national: bool = row['national_service']

    if national:
        return dict(
            primary='שירות ארצי',
            secondary=None,
            national=True
        )
    address = (resolved_address if accurate else orig_address) or orig_address
    city: str = row['branch_city'].replace('-', ' ')
    cc = regex.compile(r'\m(%s){e<2}' % city)
    m = cc.search(address.replace('-', ' '))
    if m:
        prefix = address[:m.start()].strip(' -,\n\t')
        suffix = address[m.end():].strip(' -,\n\t')
        if len(suffix) < 4:
            street_address = prefix
        else:
            street_address = prefix + ', ' + suffix
        if not accurate:
            street_address += ' (במיקום לא מדויק)'
        street_address = street_address.strip(' -,\n\t')
        return dict(
            primary=city, secondary=street_address
        )
    else:
        if accurate:
            return dict(
                primary=address, secondary=None
            )
        else:
            return dict(
                primary=address, secondary='(במיקום לא מדויק)'
            )


def org_name_parts(row):
    name: str = row['organization_name']
    short_name: str = row['organization_short_name']
    m = None
    if short_name:
        short_name = short_name.split('(')[0]
        short_name = short_name.replace(')', '').strip()
        if short_name:
            cc = regex.compile(r'\m(%s){e<2}' % short_name)
            m = cc.search(name)
    if m:
        prefix = name[: m.start()].strip(' -,\n\t')
        suffix = name[m.end() :].strip(' -,\n\t')
        name = prefix + ' ' + suffix
        name = name.strip() or None
        return dict(
            primary=short_name, secondary=name
        )
    else:
        return dict(
            primary=name, secondary=None
        )

def remove_whitespaces(resource, field):

    whitespace_re = re.compile(r'\s+', re.UNICODE | re.MULTILINE)

    def func(value):
        if isinstance(value, str):
            return whitespace_re.sub(' ', value).strip(' \t(\n-')
        return value

    return DF.Flow(
        DF.set_type(field, resources=resource, transform=func),
    )
