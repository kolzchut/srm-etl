import shutil
import dataflows as DF

from srm_tools.logger import logger
from srm_tools.processors import fetch_mapper, update_mapper
from srm_tools.stats import Stats
from srm_tools.update_table import airtable_updater
from dataflows_airtable import load_from_airtable, AIRTABLE_ID_FIELD, dump_to_airtable
from .manual_fixes import ManualFixes

from conf import settings

CHECKPOINT = 'from-curation-'


def filter_by_items(mapping, fields):
    def func(rows):
        if rows.res.name == 'current':
            yield from rows
        else:
            for row in rows:
                items = None
                for f in fields:
                    items = items or row.get(f)
                if items:
                    for i in range(len(items)):
                        item = items.pop(0)
                        if item in mapping:
                            items.append(mapping[item])
                yield row

    return func


def collect_ids(mapping):
    def func(rows):
        if rows.res.name == 'current':
            yield from rows
        else:
            for row in rows:
                mapping[row.get(AIRTABLE_ID_FIELD)] = row['id']
                yield row

    return func


def copy_from_curation_base(curation_base, source_id):
    logger.info(f'COPYING Data from {curation_base}')
    updated_orgs = dict()
    updated_branches = dict()

    table_fields = {
        settings.AIRTABLE_ORGANIZATION_TABLE: ['name', 'short_name', 'kind', 'urls', 'phone_numbers', 'email_address',
                                               'description', 'purpose'],
        settings.AIRTABLE_BRANCH_TABLE: ['name', 'organization', 'operating_unit', 'address', 'address_details',
                                         'location', 'description', 'phone_numbers', 'email_address', 'urls',
                                         'situations'],
        settings.AIRTABLE_SERVICE_TABLE: ['name', 'description', 'details', 'payment_required', 'payment_details',
                                          'urls', 'phone_numbers', 'email_address',
                                          'implements', 'situations', 'responses', 'organizations', 'branches',
                                          'responses_manual', 'situations_manual', 'data_sources', 'boost']
    }
    extra_fields = {
        settings.AIRTABLE_ORGANIZATION_TABLE: ['services', 'branch_services'],
        settings.AIRTABLE_BRANCH_TABLE: ['services', 'org_services'],
        settings.AIRTABLE_SERVICE_TABLE: ['organizations', 'branches']
    }

    for table in (settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_BRANCH_TABLE,
                  settings.AIRTABLE_SERVICE_TABLE):
        print('FIXING NEWS', curation_base, table, source_id)
        shutil.rmtree(f'{CHECKPOINT}{table}', ignore_errors=True, onerror=None)

        DF.Flow(
            load_from_airtable(curation_base, table, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.select_fields(table_fields[table] + ['decision', AIRTABLE_ID_FIELD] + ['status', 'id', 'source',
                                                                                      'fixes'] + extra_fields.get(table,
                                                                                                                  [])),
            DF.dump_to_path(CHECKPOINT + table),
            DF.filter_rows(lambda r: not r.get('decision')),
            DF.set_type('decision', transform=lambda v: v or 'New'),
            DF.select_fields(['id', 'decision', AIRTABLE_ID_FIELD], ),
            DF.update_resource(-1, name='current'),
            dump_to_airtable({
                (curation_base, table): {
                    'resource-name': 'current',
                }
            }, settings.AIRTABLE_API_KEY),
        ).process()

    manual_fixes = ManualFixes()
    stats = Stats()

    org_fields = table_fields[settings.AIRTABLE_ORGANIZATION_TABLE]
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, source_id, org_fields,
                     DF.Flow(
                         # load_from_airtable(curation_base, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
                         DF.load(CHECKPOINT + settings.AIRTABLE_ORGANIZATION_TABLE + '/datapackage.json'),
                         DF.update_resource(-1, name='orgs'),

                         stats.filter_with_stat('Data Import: Organizations: Inactive ',
                                                lambda r: r.get('status') == 'ACTIVE', resources='orgs'),
                         stats.filter_with_stat('Data Import: Organizations: Rejected/Suspended',
                                                lambda r: r.get('decision') not in ('Rejected', 'Suspended'),
                                                resources='orgs'),
                         stats.filter_with_stat('Data Import: Organizations: No Services/Branch services',
                                                lambda r: any((r.get('services'), r.get('branch_services'))),
                                                resources='orgs'),
                         manual_fixes.apply_manual_fixes(),
                         collect_ids(updated_orgs),
                         DF.delete_fields(['source', 'status'], resources=-1),
                         fetch_mapper(fields=org_fields),
                     ),
                     update_mapper()
                     )
    print('UPDATED ORGS', list(updated_orgs.values())[:10])
    conversion = dict()
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW,
                           settings.AIRTABLE_API_KEY),
        DF.filter_rows(lambda r: r.get('id') is not None),
        lambda row: conversion.setdefault(row['id'], row.get(AIRTABLE_ID_FIELD)),
    ).process()
    updated_orgs = {k: conversion.get(v) for k, v in updated_orgs.items()}

    updated_locations = dict()
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE, settings.AIRTABLE_VIEW,
                           settings.AIRTABLE_API_KEY),
        lambda row: updated_locations.setdefault(row['id'], row.get(AIRTABLE_ID_FIELD)),
    ).process()

    branch_fields = table_fields[settings.AIRTABLE_BRANCH_TABLE]
    airtable_updater(settings.AIRTABLE_BRANCH_TABLE, source_id, branch_fields,
                     DF.Flow(
                         # load_from_airtable(curation_base, settings.AIRTABLE_BRANCH_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
                         DF.load(CHECKPOINT + settings.AIRTABLE_BRANCH_TABLE + '/datapackage.json'),
                         DF.update_resource(-1, name='branches'),
                         stats.filter_with_stat('Data Import: Branches: Inactive ',
                                                lambda r: r.get('status') == 'ACTIVE', resources='branches'),
                         stats.filter_with_stat('Data Import: Branches: Rejected/Suspended',
                                                lambda r: r.get('decision') not in ('Rejected', 'Suspended'),
                                                resources='branches'),
                         stats.filter_with_stat('Data Import: Branches: No Services/Org services',
                                                lambda r: any((r.get('services'), r.get('org_services'))),
                                                resources='branches'),
                         manual_fixes.apply_manual_fixes(),
                         DF.set_type('location', type='array', transform=lambda v: [updated_locations.get(v, v)]),
                         filter_by_items(updated_orgs, ['organization']),
                         stats.filter_with_stat('Data Import: Branches: No Valid Organization',
                                                lambda r: len(r['organization'] or []) > 0),
                         collect_ids(updated_branches),
                         DF.delete_fields(['source', 'status'], resources=-1),
                         fetch_mapper(fields=branch_fields),
                     ),
                     update_mapper()
                     )
    print('UPDATED BRANCHES', list(updated_branches.values())[:10])
    conversion = dict()
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_BRANCH_TABLE, settings.AIRTABLE_VIEW,
                           settings.AIRTABLE_API_KEY),
        lambda row: conversion.setdefault(row['id'], row.get(AIRTABLE_ID_FIELD)),
    ).process()
    updated_branches = {k: conversion.get(v) for k, v in updated_branches.items()}

    service_fields = table_fields[settings.AIRTABLE_SERVICE_TABLE]
    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, source_id, service_fields,
                     DF.Flow(
                         # load_from_airtable(curation_base, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
                         DF.load(CHECKPOINT + settings.AIRTABLE_SERVICE_TABLE + '/datapackage.json'),
                         DF.update_resource(-1, name='services'),
                         stats.filter_with_stat('Data Import: Services: Inactive ',
                                                lambda r: r.get('status') == 'ACTIVE', resources='services'),
                         stats.filter_with_stat('Data Import: Services: Rejected/Suspended',
                                                lambda r: r.get('decision') not in ('Rejected', 'Suspended'),
                                                resources='services'),
                         manual_fixes.apply_manual_fixes(),
                         filter_by_items(updated_orgs, ['organizations']),
                         filter_by_items(updated_branches, ['branches']),
                         stats.filter_with_stat('Data Import: Services: No Valid Organization/Branch',
                                                lambda r: len(r['organizations'] or []) > 0 or len(
                                                    r['branches'] or []) > 0),
                         DF.delete_fields(['source', 'status'], resources=-1),
                         fetch_mapper(fields=service_fields),
                     ),
                     update_mapper()
                     )

    manual_fixes.finalize()


def operator(*_):
    logger.info('Copying data from curation tables')
    copy_from_curation_base(settings.AIRTABLE_DATA_IMPORT_BASE, 'entities')
    logger.info('Finished Copying data from curation tables')


if __name__ == '__main__':
    operator(None, None, None)
