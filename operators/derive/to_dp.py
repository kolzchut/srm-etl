from itertools import chain

import dataflows as DF
from dataflows_airtable import load_from_airtable
from thefuzz import fuzz

from conf import settings
from .autocomplete import IGNORE_SITUATIONS

from . import helpers
from .manual_fixes import ManualFixes
from .autotagging import apply_auto_tagging

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


def fix_situations(ids):
    if ids:
        both_genders = ['human_situations:gender:women', 'human_situations:gender:men']
        if all(s in ids for s in both_genders):
            ids = [s for s in ids if s not in both_genders]
        hebrew = 'human_situations:language:hebrew_speaking'
        if hebrew in ids:
            ids = [s for s in ids if s != hebrew]
    return ids


def possible_autocomplete(row):
    autocompletes = set()
    for r in row['responses']:
        autocompletes.add(r['name'])
        for s in row['situations']:
            if s['id'] not in IGNORE_SITUATIONS:
                if s['id'].split(':')[1] not in ('age_group', 'language'):
                    autocompletes.add(s['name'])
                autocompletes.add('{} עבור {}'.format(r['name'], s['name']))
            if row['branch_city']:
                autocompletes.add('שירותים עבור {} ב{}'.format(s['name'], row['branch_city']))
                autocompletes.add('{} עבור {} ב{}'.format(r['name'], s['name'], row['branch_city']))
        if row['branch_city']:
            autocompletes.add('{} ב{}'.format(r['name'], row['branch_city']))
    return sorted(set(filter(None, autocompletes)))


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

def merge_duplicate_branches(branch_mapping):
    found = dict()
    org_count = dict()

    def func(rows):
        count = 0
        for row in rows:
            count += 1

            geom = row['branch_geometry'] or [row['branch_id']]
            new_key = hasher(row['organization_id'], ';'.join(map(str, geom)))
            old_key = row['branch_key']
            branch_mapping[old_key] = new_key

            if new_key in found:
                prev_rec = found[new_key]
                for k, v in row.items():
                    if k not in ('branch_id', 'branch_key', 'branch_orig_address', 'branch_name'):
                        prev_v = prev_rec.get(k)
                        if prev_rec.get(k) != v:
                            if None in (prev_v, v):
                                prev_rec[k] = prev_v or v
                            elif isinstance(v, list):
                                for ll in v:
                                    if ll not in prev_v:
                                        prev_v.append(ll)
                            elif isinstance(v, str):
                                if (ratio := fuzz.ratio(prev_v, v)) < 80:
                                    print('DUPLICATE BRANCH FOR {}, {}: Too different in {} ({} != {} - ratio {})'.format(
                                        row['branch_id'], prev_rec['branch_id'], k, v, prev_rec.get(k), ratio
                                    ))
                            else:
                                print('DUPLICATE BRANCH FOR {}, {}: Differs in {} ({} != {})'.format(
                                    row['branch_id'], prev_rec['branch_id'], k, v, prev_rec.get(k)
                                ))
            else:
                row['branch_key'] = new_key
                found[new_key] = row
                org_count.setdefault(row['organization_id'], 0)
                org_count[row['organization_id']] += 1

        print('DEDUPLICATION: {} rows, {} unique'.format(count, len(found)))
        for value in found.values():
            value['organization_branch_count'] = org_count[value['organization_id']]
            yield value

    return DF.Flow(
        DF.add_field('organization_branch_count', 'integer'),
        func
    )

def flat_branches_flow(branch_mapping):
    """Produce a denormalized view of branch-related data."""

    return DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['branches', 'locations', 'organizations'],
        ),
        DF.update_package(name='Flat Branches'),
        DF.update_resource(['branches'], name='flat_branches', path='flat_branches.csv'),
        DF.rename_fields({'address': 'orig_address'}, resources=['flat_branches']),
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
            fields=dict(geometry=None, address=None, resolved_city=None, location_accurate=None, national_service=None),
        ),
        DF.set_type('address', transform=lambda v, row: select_address(row, ['address', 'orig_address', 'resolved_city']), resources=['flat_branches']),

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
                organization_email_address={'name': 'email_address'},
                organization_situations={'name': 'situations'},
            ),
            mode='inner'
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
                'email_address': 'branch_email_address',
                'address': 'branch_address',
                'orig_address': 'branch_orig_address',
                'resolved_city': 'branch_city',
                'geometry': 'branch_geometry',
                'location_accurate': 'branch_location_accurate',
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
                'branch_email_address',
                'branch_address',
                'branch_orig_address',
                'branch_city',
                'branch_geometry',
                'branch_location_accurate',
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
                'organization_email_address',
                'organization_situations',
                'national_service',
            ],
            resources=['flat_branches'],
        ),
        DF.validate(),
        merge_duplicate_branches(branch_mapping),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/flat_branches'),
    )


def merge_duplicate_services():

    def func(rows):
        implementing = 0
        skipped_implementing = 0
        skipped_soproc = 0
        found = dict()

        for row in rows:
            implements = row['service_implements']
            org_id = row['organization_id']
            service_id = row['service_id']
            if implements:
                # if org_id not in found:
                print('FOUND ORG WHICH IMPLEMENTS {}: {}'.format(org_id, implements))
                found.setdefault(org_id, set()).add(implements)
                implementing += 1
            else:
                if org_id in found:
                    print('ORG {} IMPLEMENTED SERVICES, CHECKING {}: {}'.format(org_id, service_id, list(found[org_id])))
                    if any(service_id in x for x in found[org_id]):
                        print('SKIPPING AS ALREADY IMPLEMENTED {}'.format(service_id))
                        skipped_implementing += 1
                        continue
                    if service_id.startswith('soproc:'):
                        print('SKIPPING AS SOPROC {}'.format(service_id))
                        skipped_soproc += 1
                        continue
            yield row
        print('DEDUPLICATION: IMPLEMENTING: {}'.format(implementing))
        print('\tSKIPPED IMPLEMENTING: {}'.format(skipped_implementing))
        print('\tSKIPPED SOPROC: {}'.format(skipped_soproc))

    return DF.Flow(
        DF.add_field('__implements', 'integer', lambda row: 0 if row['service_implements'] else 1),
        DF.sort_rows('{__implements}'),
        func,
        DF.delete_fields(['__implements']),
    )


def flat_services_flow(branch_mapping):
    """Produce a denormalized view of service-related data."""

    return DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/flat_branches/datapackage.json',
            resources=['flat_branches'],
        ),
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['services'],
        ),
        DF.update_package(name='Flat Services'),
        DF.update_resource(['services'], name='flat_services', path='flat_services.csv'),
        # branches onto services, through organizations (we already have direct branches)
        unwind('organizations', 'organization_key', resources=['flat_services']),
        DF.filter_rows(lambda r: r['national_service'] is not True, resources=['flat_branches']),
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
        DF.set_type('branches', transform=lambda v: list(set(filter(None, map(lambda i: branch_mapping.get(i), v or [])))), resources=['flat_services']),
        DF.add_field(
            'merge_branches',
            'array',
            merge_array_fields(['branches', 'organization_branches']),
            resources=['flat_services'],
        ),
        unwind('merge_branches', 'branch_key', resources=['flat_services']),
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
                'email_address': 'service_email_address',
                'implements': 'service_implements',
                'situation_ids': 'service_situations',
                'response_ids': 'service_responses',
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
                'service_email_address',
                'service_situations',
                'service_responses',
                'service_implements',
                'data_sources',
                'branch_key',
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
                branch_email_address=None,
                branch_geometry=None,
                branch_location_accurate=None,
                branch_address=None,
                branch_orig_address=None,
                branch_situations=None,
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
                organization_email_address=None,
                organization_branch_count=None,
                organization_situations=None,
                national_service=None,
            ),
            mode='inner'
        ),
        DF.filter_rows(lambda r: bool(r['service_responses']), resources=['flat_table']),
        DF.add_field(
            'branch_short_name', 'string', helpers.calculate_branch_short_name, resources=['flat_table']
        ),
        DF.set_primary_key(
            ['service_id', 'branch_id'],
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
                'service_email_address',
                'service_implements',
                'service_situations',
                'service_responses',
                'data_sources',
                'organization_id',
                'organization_name',
                'organization_short_name',
                'organization_description',
                'organization_purpose',
                'organization_kind',
                'organization_urls',
                'organization_phone_numbers',
                'organization_email_address',
                'organization_branch_count',
                'organization_situations',
                'branch_id',
                'branch_name',
                'branch_short_name',
                'branch_description',
                'branch_urls',
                'branch_phone_numbers',
                'branch_email_address',
                'branch_address',
                'branch_orig_address',
                'branch_city',
                'branch_geometry',
                'branch_location_accurate',
                'branch_situations',
                'national_service',
            ],
            resources=['flat_table'],
        ),
        DF.validate(),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/flat_table'),
    )


def card_data_flow():

    situations = DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['situations'],
        ),
        DF.select_fields(['key', 'id', 'name', 'synonyms']),
    ).results()[0][0]
    situations = dict(
        (s.pop('key'), s) for s in situations
    ) | dict(
        (s['id'], s) for s in situations
    )
    responses = DF.Flow(
        DF.load(
            f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json',
            resources=['responses'],
        ),
        DF.select_fields(['key', 'id', 'name', 'synonyms']),
    ).results()[0][0]
    responses = dict(
        (r.pop('key'), r) for r in responses
    ) | dict(
        (r['id'], r) for r in responses
    )
    def map_taxonomy(taxonomy):
        def func(ids):
            return list(map(lambda x: taxonomy[x]['id'], filter(lambda y: y in taxonomy, ids)))
        return func

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
        merge_duplicate_services(),
        DF.add_field('situation_ids', 'array', merge_array_fields(['service_situations', 'branch_situations', 'organization_situations']), resources=['card_data']),
        DF.set_type('situation_ids', transform=map_taxonomy(situations), resources=['card_data']),
        DF.set_type('situation_ids', transform=fix_situations, resources=['card_data']),
        DF.add_field('response_ids', 'array', merge_array_fields(['service_responses']), resources=['card_data']),
        DF.set_type('response_ids', transform=map_taxonomy(responses), resources=['card_data']),
        apply_auto_tagging(),

        DF.add_field('situation_ids_parents', 'array', lambda r: helpers.update_taxonomy_with_parents(r['situation_ids']), resources=['card_data']),
        DF.add_field('response_ids_parents', 'array', lambda r: helpers.update_taxonomy_with_parents(r['response_ids']), resources=['card_data']),
        DF.delete_fields(['service_situations', 'branch_situations', 'organization_situations', 'service_responses'], resources=['card_data']),
        DF.add_field('situations', 'array', lambda r: [situations[s] for s in r['situation_ids']], resources=['card_data']),
        DF.add_field('responses', 'array', lambda r: [responses[s] for s in r['response_ids']], resources=['card_data']),
        DF.add_field('situations_parents', 'array', lambda r: [situations[s] for s in r['situation_ids_parents']], resources=['card_data']),
        DF.add_field('responses_parents', 'array', lambda r: [responses[s] for s in r['response_ids_parents']], resources=['card_data']),
        DF.set_type('situation_ids', **{'es:itemType': 'string', 'es:keyword': True}, resources=['card_data']),
        DF.set_type('response_ids', **{'es:itemType': 'string', 'es:keyword': True}, resources=['card_data']),
        DF.set_type('situation_ids_parents', **{'es:itemType': 'string', 'es:keyword': True}, resources=['card_data']),
        DF.set_type('response_ids_parents', **{'es:itemType': 'string', 'es:keyword': True}, resources=['card_data']),        

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
        DF.filter_rows(lambda r: helpers.validate_geometry(r['branch_geometry']) or r['national_service'], resources=['card_data']),
        DF.add_field('possible_autocomplete', 'array', default=possible_autocomplete, resources=['card_data'], **{'es:itemType': 'string', 'es:keyword': True}),
        DF.add_field(
            'point_id', 'string',
            lambda r: helpers.calc_point_id(r['branch_geometry']) if not r['national_service'] else 'national_service',
            **{'es:keyword': True},
            resources=['card_data']
        ),
        DF.add_field(
            'national_service_details', 'string',
            lambda r: 'ארצי' if r['national_service'] else None,
        ),
        DF.add_field(
            'coords', 'string',
            lambda r: '[{},{}]'.format(*r['branch_geometry']) if r['branch_geometry'] else None,
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
        DF.update_resource(['card_data'], path='card_data.csv'),
        DF.validate(),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/card_data'),
    )


def operator(*_):
    logger.info('Starting Data Package Flow')

    branch_mapping = dict()
    srm_data_pull_flow().process()
    flat_branches_flow(branch_mapping).process()
    flat_services_flow(branch_mapping).process()
    flat_table_flow().process()
    card_data_flow().process()

    logger.info('Finished Data Package Flow')


if __name__ == '__main__':
    operator(None, None, None)
