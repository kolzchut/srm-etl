import dataflows as DF
from dataflows_airtable import load_from_airtable

from conf import settings
from srm_tools.logger import logger

from . import helpers


def srm_data_pull_flow():
    """Pull curated data from the data staging area."""
    return DF.Flow(
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_RESPONSE_TABLE, settings.AIRTABLE_VIEW
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_SITUATION_TABLE, settings.AIRTABLE_VIEW
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE, settings.AIRTABLE_VIEW
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_BRANCH_TABLE, settings.AIRTABLE_VIEW
        ),
        load_from_airtable(
            settings.AIRTABLE_BASE, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW
        ),
        DF.update_package(name='SRM Data'),
        helpers.preprocess_responses(validate=True),
        helpers.preprocess_situations(validate=True),
        helpers.preprocess_services(validate=True),
        helpers.preprocess_organizations(validate=True),
        helpers.preprocess_branches(validate=True),
        helpers.preprocess_locations(validate=True),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/srm_data'),
    )


def flat_branches_flow():
    """Produce a denormalized view of branch-related data."""

    return DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['branches', 'locations', 'organizations', 'situations'],
        ),
        DF.update_package(name='Flat Branches'),
        DF.update_resource(['branches'], name='flat_branches', path='flat_branches.csv'),
        # location onto branches
        DF.add_field('location_key', 'string', resources=['flat_branches']),
        helpers.unwind('location', 'location_key', resources=['flat_branches']),
        DF.join(
            'locations',
            ['key'],
            'flat_branches',
            ['location_key'],
            fields=dict(geometry=None, address=None),
        ),
        # organizations onto branches
        DF.add_field('organization_key', 'string', resources=['flat_branches']),
        helpers.unwind('organization', 'organization_key', resources=['flat_branches']),
        DF.join(
            'organizations',
            ['key'],
            'flat_branches',
            ['organization_key'],
            fields=dict(
                organization_key={'name': 'key'},
                organization_id={'name': 'id'},
                organization_name={'name': 'name'},
                organization_description={'name': 'description'},
                organization_purpose={'name': 'purpose'},
                organization_kind={'name': 'kind'},
                organization_urls={'name': 'urls'},
                organization_situations={'name': 'situations', 'aggregate': 'set'},
            ),
        ),
        # branch situations onto branches
        DF.duplicate('situations', '_situations'),
        DF.add_field('branch_key', 'string', resources=['_situations']),
        helpers.unwind('branches', 'branch_key', resources=['_situations']),
        DF.join(
            '_situations',
            ['branch_key'],
            'flat_branches',
            ['key'],
            fields=dict(
                branch_situation_id={'name': 'id', 'aggregate': 'set'},
                branch_situation_name={'name': 'name', 'aggregate': 'set'},
            ),
        ),
        # organization situations data onto branches
        DF.add_field('organization_key', 'string', resources=['situations']),
        helpers.unwind('organizations', 'organization_key', resources=['situations']),
        DF.join(
            'situations',
            ['organization_key'],
            'flat_branches',
            ['key'],
            fields=dict(
                organization_situation_id={'name': 'id', 'aggregate': 'set'},
                organization_situation_name={'name': 'name', 'aggregate': 'set'},
            ),
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
                'address': 'branch_address',
                'geometry': 'branch_geometry',
            },
            resources=['flat_branches'],
        ),
        DF.select_fields(
            [
                'branch_key',
                'branch_id',
                'branch_source',
                'branch_name',
                'branch_urls',
                'branch_address',
                'branch_geometry',
                'branch_situation_id',
                'branch_situation_name',
                'organization_id',
                'organization_name',
                'organization_description',
                'organization_purpose',
                'organization_kind',
                'organization_urls',
                'organization_situation_id',
                'organization_situation_name',
            ],
            resources=['flat_branches'],
        ),
        DF.validate(),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/flat_branches'),
    )


def flat_services_flow():
    """Produce a denormalized view of branch-related data."""

    return DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['services', 'branches'],
        ),
        DF.update_package(name='Flat Services'),
        DF.update_resource(['services'], name='flat_services', path='flat_services.csv'),
        DF.add_field('response_key', 'string', resources=['flat_services']),
        helpers.unwind('responses', 'response_key', resources=['flat_services']),
        DF.add_field('situation_key', 'string', resources=['flat_services']),
        helpers.unwind('situations', 'situation_key', resources=['flat_services']),
        DF.add_field('organization_key', 'string', resources=['flat_services']),
        helpers.unwind('organizations', 'organization_key', resources=['flat_services']),
        # branches through organizations
        DF.add_field('organization_key', 'string', resources=['branches']),
        helpers.unwind('organization', 'organization_key', resources=['branches']),
        DF.join(
            'branches',
            ['organization_key'],
            'flat_services',
            ['organization_key'],
            fields=dict(
                organization_branches={'name': 'key', 'aggregate': 'set'},
            ),
        ),
        # if branches has a value, then the record belongs to the branches directly
        # if branches has no value, then the record belong to the branches via the organization
        DF.add_field(
            'match_branches',
            'array',
            lambda r: r['branches'] or r['organization_branches'],
            resources=['flat_services'],
        ),
        DF.add_field('branch_key', 'string', resources=['flat_services']),
        helpers.unwind('match_branches', 'branch_key', resources=['flat_services']),
        DF.rename_fields(
            {
                'id': 'service_id',
                'name': 'service_name',
                'description': 'service_description',
                'details': 'service_details',
                'payment_required': 'service_payment_required',
                'payment_details': 'service_payment_details',
                'urls': 'service_urls',
            },
            resources=['flat_services'],
        ),
        DF.select_fields(
            [
                'service_id',
                'service_name',
                'service_description',
                'service_details',
                'service_payment_required',
                'service_payment_details',
                'service_urls',
                'response_key',
                'situation_key',
                'organization_key',
                'branch_key',
            ],
            resources=['flat_services'],
        ),
        DF.validate(),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/flat_services'),
    )


def table_data_flow():
    """Produce a table to back our Data APIs."""

    return DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['responses', 'situations', 'organizations'],
        ),
        DF.load(
            f'{settings.DATA_DUMP_DIR}/flat_branches/datapackage.json',
            resources=['flat_branches'],
        ),
        DF.load(
            f'{settings.DATA_DUMP_DIR}/flat_services/datapackage.json',
            resources=['flat_services'],
        ),
        DF.update_package(name='Table Data'),
        DF.update_resource(['flat_services'], name='table_data', path='table_data.csv'),
        DF.join(
            'responses',
            ['key'],
            'table_data',
            ['response_key'],
            fields=dict(
                response_id={'name': 'id'},
                response_name={'name': 'name'},
            ),
        ),
        DF.join(
            'situations',
            ['key'],
            'table_data',
            ['situation_key'],
            fields=dict(
                situation_id={'name': 'id'},
                situation_name={'name': 'name'},
            ),
        ),
        DF.join(
            'organizations',
            ['key'],
            'table_data',
            ['organization_key'],
            fields=dict(
                organization_id={'name': 'id'},
                organization_name={'name': 'name'},
            ),
        ),
        DF.join(
            'flat_branches',
            ['branch_key'],
            'table_data',
            ['branch_key'],
            fields=dict(
                branch_id=None,
                branch_name=None,
                branch_geometry=None,
                branch_address=None,
            ),
        ),
        DF.add_field(
            'response_parent_id',
            'string',
            lambda r: ":".join(r['response_id'].split(':')[:-1]),
            resources=['table_data'],
        ),
        DF.select_fields(
            [
                # TODO: do we want to push these forward for data provenance?
                # May be useful for future debugging, maybe not.
                # 'service_key',
                # 'response_key',
                # 'situation_key',
                # 'organization_key',
                # 'branch_key',
                'service_id',
                'service_name',
                'response_id',
                'response_name',
                'response_parent_id',
                'situation_id',
                'situation_name',
                'organization_id',
                'organization_name',
                'branch_id',
                'branch_name',
                'branch_geometry',
                'branch_address',
            ],
            resources=['table_data'],
        ),
        DF.validate(),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/table_data'),
    )


def operator(*_):

    logger.info('Starting Data Package Flow')

    # TODO - cant we run like this?
    # DF.Flow(
    #     srm_data_pull_flow(),
    #     flat_branches_flow(),
    #     flat_services_flow(),
    #     table_data_flow(),
    # ).process()

    srm_data_pull_flow().process(),
    flat_branches_flow().process(),
    flat_services_flow().process(),
    table_data_flow().process(),

    logger.info('Finished Data Package Flow')


if __name__ == '__main__':
    operator(None, None, None)
