from itertools import chain

import dataflows as DF
from dataflows_airtable import load_from_airtable

from conf import settings
from .autocomplete import IGNORE_SITUATIONS

from . import helpers
from .manual_fixes import ManualFixes

from srm_tools.logger import logger
from srm_tools.unwind import unwind
from srm_tools.hash import hasher

from operators.derive import manual_fixes


def merge_array_fields(fieldnames):
    def func(r):
        # get rid of null fields (could be None or [])
        vals = filter(None, [r[name] for name in fieldnames])
        # create a flat view over vals
        vals = chain(*vals)
        # remove duplicates
        vals = set(vals)
        # return as a sorted list
        vals = sorted(vals)
        return vals

    return func


def fix_situations(situations):
    if situations:
        ids = [s['id'] for s in situations]
        both_genders = ['human_situations:gender:women', 'human_situations:gender:men']
        if all(s in ids for s in both_genders):
            situations = [s for s in situations if s['id'] not in both_genders]
    return situations


def possible_autocomplete(row):
    autocompletes = set()
    for r in row['responses']:
        for s in row['situations']:
            autocompletes.add(r['name'])
            if s['id'] not in IGNORE_SITUATIONS:
                if s['id'].split(':')[1] not in ('age-groups', 'languages'):
                    autocompletes.add(s['name'])
                autocompletes.add('{} עבור {}'.format(r['name'], s['name']))
    return sorted(autocompletes)


def srm_data_pull_flow():
    """Pull curated data from the data staging area."""
    manual_fixes = ManualFixes()

    return DF.Flow(
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_RESPONSE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_SITUATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_BRANCH_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY
        ),
        manual_fixes.apply_manual_fixes(),
        DF.update_package(name='SRM Data'),
        helpers.preprocess_responses(validate=True),
        helpers.preprocess_situations(validate=True),
        helpers.preprocess_services(validate=True),
        helpers.preprocess_organizations(validate=True),
        helpers.preprocess_branches(validate=True),
        helpers.preprocess_locations(validate=True),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/srm_data'),
    )


def select_address(row, address_fields):
    for f in address_fields:
        v = row.get(f)
        if helpers.validate_address(v):
            return row[f]


def flat_branches_flow():
    """Produce a denormalized view of branch-related data."""

    return DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['branches', 'locations', 'organizations'],
        ),
        DF.update_package(name='Flat Branches'),
        DF.update_resource(['branches'], name='flat_branches', path='flat_branches.csv'),
        DF.rename_fields({'address': 'branch_address'}, resources=['flat_branches']),
        # location onto branches
        DF.filter_rows(
            lambda r: r['location'] and len(r['location']) > 0, resources=['flat_branches']
        ),
        DF.add_field(
            'location_key',
            'string',
            lambda r: r['location'][0],
            resources=['flat_branches'],
        ),
        DF.join(
            'locations',
            ['key'],
            'flat_branches',
            ['location_key'],
            fields=dict(geometry=None, address=None, resolved_city=None),
        ),
        DF.set_type('address', transform=lambda v, row: select_address(row, ['address', 'branch_address', 'resolved_city'])),
        DF.delete_fields(['branch_address'], resources=['flat_branches']),
        # organizations onto branches
        DF.add_field(
            'organization_key',
            'string',
            lambda r: r['organization'][0],
            resources=['flat_branches'],
        ),
        DF.join(
            'organizations',
            ['key'],
            'flat_branches',
            ['organization_key'],
            fields=dict(
                organization_key={'name': 'key'},
                organization_id={'name': 'id'},
                organization_name={'name': 'name'},
                organization_short_name={'name': 'short_name'},
                organization_description={'name': 'description'},
                organization_purpose={'name': 'purpose'},
                organization_kind={'name': 'kind'},
                organization_urls={'name': 'urls'},
                organization_phone_numbers={'name': 'phone_numbers'},
                organization_situations={'name': 'situations', 'aggregate': 'set'},
            ),
            mode='inner'
        ),
        # merge multiple situation fields into a single field
        DF.add_field(
            'merged_situations',
            'array',
            merge_array_fields(['situations', 'organization_situations']),
            resources=['flat_branches'],
        ),
        DF.rename_fields(
            {
                'key': 'branch_key',
                'id': 'branch_id',
                'source': 'branch_source',
                'name': 'branch_name',
                'description': 'branch_description',
                'urls': 'branch_urls',
                'phone_numbers': 'branch_phone_numbers',
                'email_addresses': 'branch_email_addresses',
                'address': 'branch_address',
                'resolved_city': 'branch_city',
                'geometry': 'branch_geometry',
                'situations': 'branch_situations',
            },
            resources=['flat_branches'],
        ),
        DF.select_fields(
            [
                'branch_key',
                'branch_id',
                'branch_source',
                'branch_name',
                'branch_description',
                'branch_urls',
                'branch_phone_numbers',
                'branch_email_addresses',
                'branch_address',
                'branch_city',
                'branch_geometry',
                'branch_situations',
                'organization_key',
                'organization_id',
                'organization_name',
                'organization_short_name',
                'organization_description',
                'organization_purpose',
                'organization_kind',
                'organization_urls',
                'organization_phone_numbers',
                'organization_situations',
                'merged_situations',
            ],
            resources=['flat_branches'],
        ),
        DF.validate(),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/flat_branches'),
    )


def flat_services_flow():
    """Produce a denormalized view of service-related data."""

    return DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/flat_branches/datapackage.json',
            resources=['flat_branches'],
        ),
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['responses', 'services'],
        ),
        DF.update_package(name='Flat Services'),
        DF.update_resource(['services'], name='flat_services', path='flat_services.csv'),
        # responses onto services
        unwind('response_ids', 'response_id', resources=['flat_services']),
        DF.join(
            'responses',
            ['id'],
            'flat_services',
            ['response_id'],
            fields=dict(
                response_id={'name': 'id'},
                response_name={'name': 'name'},
                response_situations={'name': 'situations'},
                response_synonyms={'name': 'synonyms'},
            ),
        ),
        # branches onto services, through organizations (we already have direct branches)
        unwind('organizations', 'organization_key', resources=['flat_services']),
        DF.join(
            'flat_branches',
            ['organization_key'],
            'flat_services',
            ['organization_key'],
            fields=dict(
                organization_branches={'name': 'branch_key', 'aggregate': 'set'},
            ),
        ),
        # merge multiple branch fields into a single field
        DF.add_field(
            'merge_branches',
            'array',
            merge_array_fields(['branches', 'organization_branches']),
            resources=['flat_services'],
        ),
        unwind('merge_branches', 'branch_key', resources=['flat_services']),
        # merge multiple situation fields into a single field
        DF.add_field(
            'merged_situations',
            'array',
            merge_array_fields(['situations', 'response_situations']),
            resources=['flat_services'],
        ),
        DF.rename_fields(
            {
                'key': 'service_key',
                'id': 'service_id',
                'name': 'service_name',
                'description': 'service_description',
                'details': 'service_details',
                'payment_required': 'service_payment_required',
                'payment_details': 'service_payment_details',
                'urls': 'service_urls',
                'phone_numbers': 'service_phone_numbers',
                'situations': 'service_situations',
            },
            resources=['flat_services'],
        ),
        DF.select_fields(
            [
                'service_key',
                'service_id',
                'service_name',
                'service_description',
                'service_details',
                'service_payment_required',
                'service_payment_details',
                'service_urls',
                'service_phone_numbers',
                'service_situations',
                'data_sources',
                'response_key',
                'response_id',
                'response_name',
                'response_situations',
                'response_synonyms',
                'branch_key',
                'merged_situations',
            ],
            resources=['flat_services'],
        ),
        DF.validate(),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/flat_services'),
    )


def flat_table_flow():
    """Produce a flat table to back our Data APIs."""

    return DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['situations'],
        ),
        DF.load(
            f'{settings.DATA_DUMP_DIR}/flat_branches/datapackage.json',
            resources=['flat_branches'],
        ),
        DF.load(
            f'{settings.DATA_DUMP_DIR}/flat_services/datapackage.json',
            resources=['flat_services'],
        ),
        DF.update_package(name='Flat Table'),
        DF.update_resource(['flat_services'], name='flat_table', path='flat_table.csv'),
        DF.join(
            'flat_branches',
            ['branch_key'],
            'flat_table',
            ['branch_key'],
            fields=dict(
                branch_id=None,
                branch_name=None,
                branch_description=None,
                branch_urls=None,
                branch_phone_numbers=None,
                branch_email_addresses=None,
                branch_geometry=None,
                branch_address=None,
                branch_city=None,
                organization_key=None,
                organization_id=None,
                organization_name=None,
                organization_short_name=None,
                organization_description=None,
                organization_purpose=None,
                organization_kind=None,
                organization_urls=None,
                organization_phone_numbers=None,
                branch_merged_situations={'name': 'merged_situations'},
            ),
            mode='inner'
        ),
        DF.filter_rows(lambda r: r['response_id'] is not None, resources=['flat_table']),
        DF.add_field(
            'branch_short_name', 'string', helpers.calculate_branch_short_name, resources=['flat_table']
        ),
        DF.add_field(
            'response_category',
            'string',
            lambda r: r['response_id'].split(':')[1],
            resources=['flat_table'],
        ),
        # merge multiple situation fields into a single field
        DF.add_field(
            'situations',
            'array',
            merge_array_fields(['branch_merged_situations', 'merged_situations']),
            resources=['flat_table'],
        ),
        # situations onto table records
        unwind('situations', 'situation_key', resources=['flat_table'], allow_empty=True),
        DF.join(
            'situations',
            ['key'],
            'flat_table',
            ['situation_key'],
            fields=dict(
                situation_id={'name': 'id'},
                situation_name={'name': 'name'},
                situation_synonyms={'name': 'synonyms'},
            ),
        ),
        DF.set_primary_key(
            ['service_id', 'response_id', 'branch_id', 'situation_id'],
            resources=['flat_table'],
        ),
        DF.select_fields(
            [
                # Keys from airtable may be useful for future debugging/provenance.
                'service_key',
                'response_key',
                'situation_key',
                'organization_key',
                'branch_key',
                # fields for our API
                'service_id',
                'service_name',
                'service_description',
                'service_details',
                'service_payment_required',
                'service_payment_details',
                'service_urls',
                'service_phone_numbers',
                'data_sources',
                'response_id',
                'response_name',
                'response_synonyms',
                'response_category',
                'organization_id',
                'organization_name',
                'organization_short_name',
                'organization_description',
                'organization_purpose',
                'organization_kind',
                'organization_urls',
                'organization_phone_numbers',
                'branch_id',
                'branch_name',
                'branch_short_name',
                'branch_description',
                'branch_urls',
                'branch_phone_numbers',
                'branch_email_addresses',
                'branch_address',
                'branch_city',
                'branch_geometry',
                'situation_id',
                'situation_name',
                'situation_synonyms',
            ],
            resources=['flat_table'],
        ),
        DF.validate(),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/flat_table'),
    )


def card_data_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/flat_table/datapackage.json'),
        DF.update_package(name='Card Data'),
        DF.update_resource(['flat_table'], name='card_data', path='card_data.csv'),
        DF.add_field(
            'card_id',
            'string',
            lambda r: hasher(r['branch_id'], r['service_id']),
            resources=['card_data'],
        ),
        DF.join_with_self(
            'card_data',
            ['card_id'],
            fields=dict(
                card_id=None,
                service_id=None,
                service_name=None,
                service_description=None,
                service_details=None,
                service_payment_required=None,
                service_payment_details=None,
                service_urls=None,
                service_phone_numbers=None,
                data_sources=None,
                response_id={'name': 'response_id', 'aggregate': 'array'},
                response_name={'name': 'response_name', 'aggregate': 'array'},
                response_synonyms={'name': 'response_synonyms', 'aggregate': 'array'},
                response_categories={'name': 'response_category', 'aggregate': 'set'},
                organization_id=None,
                organization_name=None,
                organization_short_name=None,
                organization_description=None,
                organization_purpose=None,
                organization_kind=None,
                organization_urls=None,
                organization_phone_numbers=None,
                branch_id=None,
                branch_name=None,
                branch_short_name=None,
                branch_description=None,
                branch_urls=None,
                branch_phone_numbers=None,
                branch_email_addresses=None,
                branch_address=None,
                branch_city=None,
                branch_geometry=None,
                situation_id={'name': 'situation_id', 'aggregate': 'array'},
                situation_name={'name': 'situation_name', 'aggregate': 'array'},
                situation_synonyms={'name': 'situation_synonyms', 'aggregate': 'array'},
            ),
        ),
        DF.add_field(
            'situations',
            'array',
            lambda r: [
                {'id': id, 'name': name, 'synonyms': synonyms}
                for id, name, synonyms in set(tuple(zip(r['situation_id'], r['situation_name'], map(tuple, r['situation_synonyms']))))
            ],
            resources=['card_data'],
        ),
        DF.set_type('situations', transform=fix_situations, resources=['card_data']),
        DF.add_field(
            'responses',
            'array',
            lambda r: [
                {'id': id, 'name': name, 'synonyms': synonyms}
                for id, name, synonyms in set(tuple(zip(r['response_id'], r['response_name'], map(tuple, r['response_synonyms']))))
            ],
            resources=['card_data'],
        ),
        DF.add_field(
            'response_ids', 'array', 
            lambda r: helpers.update_taxonomy_with_parents(r['response_id']),
            **{'es:itemType': 'string', 'es:keyword': True},
            resources=['card_data']
        ),
        DF.add_field(
            'situation_ids', 'array',
            lambda r: helpers.update_taxonomy_with_parents([s['id'] for s in r['situations']]),
            **{'es:itemType': 'string', 'es:keyword': True},
            resources=['card_data']
        ),
        DF.add_field(
            'response_categories',
            'array',
            lambda r: [r['id'].split(':')[1] for r in r['responses']],
            **{'es:itemType': 'string', 'es:keyword': True},
            resources=['card_data'],
        ),
        DF.add_field(
            'response_category',
            'string',
            helpers.most_common_category,
            resources=['card_data'],
            **{'es:keyword': True},
        ),
        DF.set_type('responses', transform=lambda v, row: helpers.reorder_responses_by_category(v, row['response_category'])),
        DF.filter_rows(lambda r: helpers.validate_geometry(r['branch_geometry']), resources=['card_data']),
        DF.add_field('possible_autocomplete', 'array', default=possible_autocomplete, resources=['card_data'], **{'es:itemType': 'string', 'es:keyword': True}),
        DF.add_field(
            'point_id', 'string',
            lambda r: helpers.calc_point_id(r['branch_geometry']),
            **{'es:keyword': True},
            resources=['card_data']
        ),
        DF.add_field(
            'collapse_key', 'string',
            lambda r: r['organization_id'] + ':' + r['service_name'],
            **{'es:keyword': True},
            resources=['card_data']
        ),
        DF.add_field(
            'address_parts', 'object', helpers.address_parts,
            **{'es:schema': dict(fields=[
                dict(name='primary', type='string'),
                dict(name='secondary', type='string'),
            ])}
        ),
        DF.add_field(
            'organization_name_parts', 'object', helpers.org_name_parts,
            **{'es:schema': dict(fields=[
                dict(name='primary', type='string'),
                dict(name='secondary', type='string'),
            ])}
        ),
        DF.set_type('card_id', **{'es:keyword': True}),
        DF.set_type('branch_id', **{'es:keyword': True}),
        DF.set_type('service_id', **{'es:keyword': True}),
        DF.set_type('organization_id', **{'es:keyword': True}),
        DF.set_type('response_categories', **{'es:itemType': 'string', 'es:keyword': True}),
        DF.set_primary_key(['card_id'], resources=['card_data']),
        DF.delete_fields(
            [
                'response_id',
                'response_name',
                'response_synonyms',
                'situation_id',
                'situation_name',
                'situation_synonyms',
            ],
            resources=['card_data'],
        ),
        # TODO - When we join with self (in some cases??), it puts the resource into a path under data/
        # this workaround just keeps behaviour same as other dumps we have.
        DF.update_resource(['card_data'], path='card_data.csv'),
        DF.validate(),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/card_data'),
    )


def operator(*_):
    logger.info('Starting Data Package Flow')

    srm_data_pull_flow().process()
    flat_branches_flow().process()
    flat_services_flow().process()
    flat_table_flow().process()
    card_data_flow().process()

    logger.info('Finished Data Package Flow')


if __name__ == '__main__':
    operator(None, None, None)
