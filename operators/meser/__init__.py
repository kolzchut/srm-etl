import os
from pathlib import Path
import tempfile
import re

from openlocationcode import openlocationcode as olc
from pyproj import Transformer

import dataflows as DF
from dataflows_airtable import load_from_airtable, dump_to_airtable

from srm_tools.processors import update_mapper
from srm_tools.logger import logger
from srm_tools.stats import Stats
from srm_tools.update_table import airtable_updater
from srm_tools.unwind import unwind
from srm_tools.hash import hasher
from srm_tools.datagovil import fetch_datagovil
from srm_tools.error_notifier import invoke_on
from update_organization_meser import update_organization_meser_id
from conf import settings

transformer = Transformer.from_crs('EPSG:2039', 'EPSG:4326', always_xy=True)
noOrgIdCount = 0
badOrgIdLengthCount = {}


def alternate_address(row):
    assert '999' not in row['address'], str(row)
    bad_address = any(not row[f] for f in ['Adrees', 'City_Name'])
    has_coord = all(row[f] for f in ['GisX', 'GisY'])
    if bad_address and has_coord:
        x = int(row['GisX'])
        y = int(row['GisY'])
        lon, lat = transformer.transform(x, y)
        pc = olc.encode(lat, lon, 11)
        return pc
    return row['address'].strip()


def good_company(r):
    global noOrgIdCount, badOrgIdLengthCount

    is_org_id = r['organization_id'] is not None
    if not is_org_id:
        noOrgIdCount += 1
    print(orgId := r['organization_id'])
    is_length_good = is_org_id and len(r['organization_id']) == 9
    if is_org_id and not is_length_good:
        orgLength = len(r['organization_id'])
        if orgLength not in badOrgIdLengthCount:
            badOrgIdLengthCount[orgLength] = 0
        badOrgIdLengthCount[orgLength] += 1

    return is_org_id and is_length_good


def flatten_and_deduplicate(values):
    """Flatten nested (lists/tuples/generators) of tag strings.
    Splits on commas and whitespace, preserves order and uniqueness.
    Warns if a single string contains multiple human_situations: prefixes.
    """
    flat = []
    if values is None:
        return flat
    for item in values:
        if item is None:
            continue
        if isinstance(item, (list, tuple)):
            flat.extend(flatten_and_deduplicate(item))
            continue
        if not isinstance(item, str):
            # Coerce other scalars to string just in case
            item = str(item)
        if item.count('human_situations:') > 1:
            logger.warning(f'composite human_situations string encountered: {item}')
        # Split by any run of whitespace or commas
        parts = [p for p in re.split(r'[\s,]+', item.strip()) if p]
        flat.extend(parts)
    # Deduplicate preserving order
    seen = set()
    ordered = []
    for t in flat:
        if t not in seen:
            seen.add(t)
            ordered.append(t)
    return ordered


def run(*_):
    logger.info('Starting Meser Data Flow')

    stats = Stats()

    tags = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, 'meser-tagging', settings.AIRTABLE_VIEW,
                           settings.AIRTABLE_API_KEY),
        DF.filter_rows(lambda r: r.get('tag') not in (None, 'dummy')),
        DF.select_fields(['tag', 'response_ids', 'situation_ids']),
    ).results()[0][0]
    tags = {r.pop('tag'): r for r in tags}

    with tempfile.TemporaryDirectory(prefix='meser_') as td:
        os.chmod(td, 0o755)
        dirname = td
        source_data = os.path.join(td, 'data.csv')

        fetch_datagovil('welfare-frames', 'מסגרות רווחה', source_data)


        DF.Flow(
            # Loading data
            DF.load(str(source_data), infer_strategy=DF.load.INFER_STRINGS, headers=1),
            DF.update_resource(-1, name='meser'),
            DF.select_fields(['Name',
                              'Misgeret_Id', 'Type_Descr', 'Target_Population_Descr', 'Head_Department',
                              'Second_Classific',
                              'ORGANIZATIONS_BUSINES_NUM', 'Registered_Business_Id',
                              'Gender_Descr', 'City_Name', 'Adrees', 'Telephone', 'GisX', 'GisY']),
            # Cleanup
            DF.update_schema(-1, missingValues=['NULL', '-1', 'לא ידוע', 'לא משויך', 'רב תכליתי', '0', '999', '9999']),
            DF.validate(),
            # Adding fields
            DF.add_field('service_name', 'string', lambda r: r['Name'].strip()),
            DF.add_field('branch_name', 'string', lambda r: r['Type_Descr'].strip()),
            DF.add_field('service_description', 'string',
                         lambda r: r['Type_Descr'].strip() + (' עבור ' + r['Target_Population_Descr'].strip()) if r[
                             'Target_Population_Descr'] else ''),
            DF.add_field('organization_id', 'string',
                         lambda r: r['ORGANIZATIONS_BUSINES_NUM'] or r['Registered_Business_Id'] or '500106406'),
            DF.set_type('Adrees', type='string', transform=lambda v: v.replace('999', '').strip()),
            DF.set_type('Adrees', type='string', transform=lambda v, row: v if v != row['City_Name'] else None),
            DF.add_field('address', 'string',
                         lambda r: ' '.join(filter(None, [r['Adrees'], r['City_Name']])).replace(' - ', '-')),
            DF.add_field('branch_id', 'string', lambda r: 'meser-' + hasher(r['address'], r['organization_id'])),
            DF.add_field('location', 'string', alternate_address),
            DF.add_field('tagging', 'array', lambda r: list(filter(None, [r['Type_Descr'], r['Target_Population_Descr'],
                                                                          r['Second_Classific'], r['Gender_Descr'],
                                                                          r['Head_Department']]))),
            DF.add_field('phone_numbers', 'string',
                         lambda r: '0' + r['Telephone'] if r['Telephone'] and r['Telephone'][0] != '0' else r[
                                                                                                                'Telephone'] or None),

            DF.add_field('meser_id', 'string', lambda r: r['Misgeret_Id']),

            # Combining same services
            DF.add_field('service_id', 'string',
                         lambda r: 'meser-' + hasher(r['service_name'], r['phone_numbers'], r['address'],
                                                     r['organization_id'], r['branch_id'])),
            DF.join_with_self('meser', ['service_id'], fields=dict(
                service_id=None,
                service_name=None,
                service_description=None,
                branch_id=None,
                branch_name=None,
                organization_id=None,
                address=None,
                location=None,
                tagging=dict(aggregate='array'),
                phone_numbers=None
            )),
            DF.set_type('tagging', type='array', transform=lambda v: list(set(vvv for vv in v for vvv in vv))),

            # Adding tags
            DF.add_field(
                'responses', 'array',
                lambda r: flatten_and_deduplicate(
                    resp
                    for t in r['tagging']
                    for resp in (tags.get(t, {}).get('response_ids') or [])
                )
            ),

            DF.add_field(
                'situations', 'array',
                lambda r: flatten_and_deduplicate(
                    sit
                    for t in r['tagging']
                    for sit in (tags.get(t, {}).get('situation_ids') or [])
                )
            ),

            stats.filter_with_stat('MESER: No Org Id', good_company),

            DF.dump_to_path(os.path.join(dirname, 'meser', 'denormalized')),
        ).process()

        meser_folder = os.path.join(dirname)
        update_organization_meser_id(meser_folder)

        airtable_updater(
            settings.AIRTABLE_ORGANIZATION_TABLE,
            'entities', ['id'],
            DF.Flow(
                DF.load(os.path.join(dirname, 'meser', 'denormalized', 'datapackage.json')),
                DF.join_with_self('meser', ['organization_id'], fields=dict(organization_id=None)),
                DF.rename_fields({'organization_id': 'id'}, resources='meser'),
                DF.add_field('data', 'object', lambda r: dict(id=r['id'])),
                DF.printer()
            ),
            update_mapper(),
            manage_status=False,
            airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
        )

        airtable_updater(
            settings.AIRTABLE_BRANCH_TABLE,
            'meser', ['id', 'name', 'organization', 'location', 'address', 'phone_numbers'],
            DF.Flow(
                DF.load(os.path.join(dirname, 'meser', 'denormalized', 'datapackage.json')),
                DF.join_with_self('meser', ['branch_id'], fields=dict(
                    branch_id=None, branch_name=None, organization_id=None, address=None, location=None,
                    phone_numbers=None)
                                  ),
                DF.rename_fields({
                    'branch_id': 'id',
                }, resources='meser'),
                DF.add_field('name', 'string', lambda r: '', resources='meser'),
                DF.add_field('organization', 'array', lambda r: [r['organization_id']], resources='meser'),
                DF.add_field('data', 'object', lambda r: dict((k, v) for k, v in r.items() if k != 'id'),
                             resources='meser'),
                DF.printer()
            ),
            update_mapper(),
            airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
        )

        airtable_updater(
            settings.AIRTABLE_SERVICE_TABLE,
            'meser', ['id', 'name', 'description', 'data_sources', 'situations', 'responses', 'branches'],
            DF.Flow(
                DF.load(os.path.join(dirname, 'meser', 'denormalized', 'datapackage.json')),
                DF.rename_fields({
                    'service_id': 'id',
                    'service_name': 'name',
                    'service_description': 'description',
                }, resources='meser'),
                DF.add_field('data_sources', 'string', 'מידע על מסגרות רווחה התקבל ממשרד הרווחה והשירותים החברתיים',
                             resources='meser'),
                DF.add_field('branches', 'array', lambda r: [r['branch_id']], resources='meser'),
                DF.select_fields(['id', 'name', 'description', 'data_sources', 'situations', 'responses', 'branches'],
                                 resources='meser'),
                DF.add_field('data', 'object', lambda r: dict((k, v) for k, v in r.items() if k != 'id'),
                             resources='meser'),
                DF.printer()
            ),
            update_mapper(),
            airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
        )
        DF.Flow(
            DF.load(os.path.join(dirname, 'meser', 'denormalized', 'datapackage.json')),
            DF.update_resource(-1, name='tagging'),
            DF.select_fields(['tagging', 'Misgeret_Id']),
            unwind('tagging', 'tag'),
            DF.join_with_self('tagging', ['tag'], fields=dict(tag=None)),
            DF.filter_rows(lambda r: r['tag'] not in tags),
            DF.filter_rows(lambda r: bool(r['tag'])),
            DF.add_field('meser_id', 'string', lambda r: r['Misgeret_Id']),
            dump_to_airtable({
                (settings.AIRTABLE_DATA_IMPORT_BASE, 'meser-tagging'): {
                    'resource-name': 'tagging',
                }
            }, settings.AIRTABLE_API_KEY)
        ).process()

        logger.info("No org id Count: %s", noOrgIdCount)
        logger.info("bad org id length Count: %s", badOrgIdLengthCount)

        logger.info('Finished Meser Data Flow')


def operator(*_):
    invoke_on(run, 'Meser')


if __name__ == '__main__':
    run(None, None, None)
