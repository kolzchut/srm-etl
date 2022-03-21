from collections import Counter
import dataflows as DF
from dataflows.helpers.resource_matcher import ResourceMatcher


def transform_urls(urls):
    def transformer(s):
        href, title = s.rsplit('#', 1)
        return {'href': href, 'title': title}

    return list(map(transformer, urls.split('\n'))) if urls else None


def transform_email_addresses(email_addresses):
    return email_addresses.split(',') if email_addresses else None


def transform_phone_numbers(phone_numbers):
    return phone_numbers.split(',') if phone_numbers else None


def calc_point_id(geometry):
    return ''.join('{:08.5f}'.format(c) for c in geometry).replace('.','')


def validate_geometry(geometry):
    if geometry:
        if len(geometry) == 2:
            # Y: 29.3-33.3
            # X: 33-37
            if 33 < geometry[0] < 37 and 29.3 < geometry[1] < 33.3:
                return True
    return False

def unwind(
    from_key, to_key, to_key_type='string',
    transformer=None, resources=None, source_delete=True, allow_empty=None
):

    """From a row of data, generate a row per value from from_key, where the value is set onto to_key."""
    from dataflows.processors.add_computed_field import get_new_fields

    def _unwinder(rows):
        for row in rows:
            try:
                values = row[from_key]
                iter(values)
                if allow_empty and len(values) == 0:
                    values = [None]
                for value in values:
                    ret = {}
                    ret.update(row)
                    ret[to_key] = value
                    if source_delete is True:
                        del ret[from_key]
                    yield ret
            except TypeError:
                # no iterable to unwind. Take the value we have and set it on the to_key.
                ret = {}
                ret.update(row)
                ret[to_key] = ret[from_key] if transformer is None else transformer(ret[from_key])
                if source_delete is True:
                    del ret[from_key]
                yield ret

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        for resource in package.pkg.descriptor['resources']:
            if matcher.match(resource['name']):
                new_fields = get_new_fields(
                    resource, [{'target': {'name': to_key, 'type': to_key_type}}]
                )
                resource['schema']['fields'] = [
                    field
                    for field in resource['schema']['fields']
                    if not source_delete or not field['name'] == from_key
                ]
                resource['schema']['fields'].extend(new_fields)

        yield package.pkg

        for resource in package:
            if matcher.match(resource.res.name):
                yield _unwinder(resource)
            else:
                yield resource

    return func


def filter_dummy_data():
    return DF.filter_rows(lambda r: not any([r.get('id') == 'dummy', r.get('name') == 'dummy']))


def filter_active_data():
    return DF.filter_rows(lambda r: r['status'] != 'INACTIVE')


def set_staging_pkey(resource_name):
    return DF.rename_fields({'__airtable_id': 'key'}, resources=[resource_name])


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
        filter_active_data(),
        set_staging_pkey('responses'),
        DF.select_fields(select_fields, resources=['responses']) if select_fields else None,
        DF.validate() if validate else None,
    )


def preprocess_situations(select_fields=None, validate=False):
    return DF.Flow(
        DF.update_resource(['Situations'], name='situations', path='situations.csv'),
        filter_dummy_data(),
        filter_active_data(),
        set_staging_pkey('situations'),
        DF.select_fields(select_fields, resources=['situations']) if select_fields else None,
        DF.validate() if validate else None,
    )


def preprocess_services(select_fields=None, validate=False):
    return DF.Flow(
        DF.update_resource(['Services'], name='services', path='services.csv'),
        filter_dummy_data(),
        filter_active_data(),
        set_staging_pkey('services'),
        # DF.filter_rows(
        #     lambda r: r['selected'] is True or r['source'] == 'guidestar', resources=['services']
        # ),
        DF.set_type('urls', type='array', transform=transform_urls, resources=['services']),
        DF.set_type('name', transform=lambda v, row: row['name_manual'] or v, resources=['services']),
        DF.set_type('responses', transform=lambda _, row: row['responses_manual'] if row['source'] in ('social-procurement', 'shil') else row['responses'], resources=['services']),
        DF.set_type('situations', transform=lambda _, row: row['situations_manual'] if row['source'] in ('social-procurement', 'shil') else row['situations'], resources=['services']),
        DF.add_field('response_ids', 'array', transform=lambda _, row: row['responses_manual_ids'] if row['source'] in ('social-procurement', 'shil') else row['responses_ids'], resources=['services']),
        DF.delete_fields(['situations_manual', 'responses_manual', 'name_manual', 'responses_manual_ids', 'responses_ids'], resources=['services']),
        DF.select_fields(select_fields, resources=['services']) if select_fields else None,
        DF.validate() if validate else None,
    )


def preprocess_organizations(select_fields=None, validate=False):
    return DF.Flow(
        DF.update_resource(['Organizations'], name='organizations', path='organizations.csv'),
        filter_dummy_data(),
        filter_active_data(),
        set_staging_pkey('organizations'),
        DF.set_type('urls', type='array', transform=transform_urls, resources=['organizations']),
        DF.select_fields(select_fields, resources=['organizations']) if select_fields else None,
        DF.validate() if validate else None,
    )


def preprocess_branches(select_fields=None, validate=False):
    return DF.Flow(
        DF.update_resource(['Branches'], name='branches', path='branches.csv'),
        filter_dummy_data(),
        filter_active_data(),
        set_staging_pkey('branches'),
        DF.set_type('urls', type='array', transform=transform_urls, resources=['branches']),
        DF.set_type(
            'phone_numbers',
            type='array',
            transform=transform_phone_numbers,
            resources=['branches'],
        ),
        DF.set_type(
            'email_addresses',
            type='array',
            transform=transform_email_addresses,
            resources=['branches'],
        ),
        DF.select_fields(select_fields, resources=['branches']) if select_fields else None,
        DF.validate() if validate else None,
    )


def preprocess_locations(select_fields=None, validate=False):
    return DF.Flow(
        DF.update_resource(['Locations'], name='locations', path='locations.csv'),
        filter_dummy_data(),
        set_staging_pkey('locations'),
        DF.filter_rows(
            lambda r: any(
                all(r.get(f) for f in fields)
                for fields in [('resolved_lat', 'resolved_lon'), ('fixed_lat', 'fixed_lon')]
            ),
            resources=['locations'],
        ),
        DF.filter_rows(
            lambda r: r['accuracy'] in ('ROOFTOP', 'RANGE_INTERPOLATED', 'STREET_MID_POINT', 'POI_MID_POINT', 'ADDR_V1'),
            resources=['locations'],
        ),
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
            'geometry', 'geopoint', lambda r: [r['lon'], r['lat']], resources=['locations']
        ),
        DF.add_field(
            'address',
            'string',
            lambda r: r.get('resolved_address') or r['id'],
            resources=['locations'],
        ),
        DF.select_fields(select_fields, resources=['locations']) if select_fields else None,
    )


def point_offset_table():
    """Lookup table for positioning up to seven points."""
    # https://github.com/whiletrue-industries/srm-etl/issues/8
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
        print('ERROR: no response categories', repr(row))
        return None
    return Counter(response_categories).most_common(1)[0][0]
