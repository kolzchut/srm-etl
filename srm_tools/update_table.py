import dataflows as DF

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from srm_tools.hash import hasher

from conf import settings


def airtable_updater(table, source_id, table_fields, fetch_data_flow, update_data_flow, manage_status=True, airtable_base=None):
    """
    Updates the given airtable table with new data, maintaining status correctly.
    :param table: The table to update.
    :param source_id: The source value to check for and use for new data.
    :param table_fields: The fields in the airtable table that we want to update (except the id, source and status fields).
    :param fetch_data_flow: Flow which will be used to fetch new data. 
        Needs to add a new resource with two fields - 'id' (unique row id) and 'data' (object with the newly fetched data).
    :param update_data_flow: Flow to use to map the 'data' field into the table standard fields.
    """
    dp, stats = DF.Flow(
        airtable_updater_flow(table, source_id, table_fields, fetch_data_flow, update_data_flow, manage_status=manage_status, airtable_base=airtable_base),
        # DF.printer()
    ).process()
    print(f'AIRTABLE UPDATER {table} ({source_id})', dp.descriptor)


def hash_row(table_fields):
    def func(row):
        return '###'.join([str(row.get(f)) for f in table_fields + ['source', 'status']]).replace('\n', '').replace(' ', '').replace('\t', '')
    return func


def test_hash(table_fields):
    def func(rows: DF.ResourceWrapper):
        count_existing = 0
        count_new = 0
        count_different = 0
        for row in rows:
            h = row.get('_current_hash')
            if h:
                count_existing += 1
                new_h = hash_row(table_fields)(row)
                if h != new_h:
                    # if count_different < 10:
                    #     print('DIFFERENT\n', h, '\n', new_h)
                    count_different += 1
                    yield row
            else:
                count_new += 1
                yield row
        print(f'{rows.res.name} -- Existing: {count_existing}, New: {count_new}, Different: {count_different}')
    return func

def airtable_updater_flow(table, source_id, table_fields, fetch_data_flow, update_data_flow, manage_status=True, airtable_base=None):
    """
    Updates the given airtable table with new data, maintaining status correctly.
    :param table: The table to update.
    :param source_id: The source value to check for and use for new data.
    :param table_fields: The fields in the airtable table that we want to update (except the id, source and status fields).
    :param fetch_data_flow: Flow which will be used to fetch new data. 
        Needs to add a new resource with two fields - 'id' (unique row id) and 'data' (object with the newly fetched data).
    :param update_data_flow: Flow to use to map the 'data' field into the table standard fields.
    """
    print('UPDATING', airtable_base or settings.AIRTABLE_BASE, table, source_id, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY)
    return DF.Flow(
        load_from_airtable(airtable_base or settings.AIRTABLE_BASE, table, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='current'),
        DF.validate(),
        DF.filter_rows(lambda r: r.get('source') in (source_id, 'dummy'), resources='current'),

        DF.add_field('_current_hash', 'string', hash_row(table_fields), resources='current'),

        fetch_data_flow,
        DF.update_resource(-1, name='fetched'),

        DF.join('current', ['id'], 'fetched', ['id'], dict(
            (f, None) for f in [
                *table_fields, AIRTABLE_ID_FIELD, '_current_hash'
            ]
        ), mode='full-outer' if manage_status else 'half-outer'),

        DF.add_field('status', 'string', lambda r: 'ACTIVE' if r.get('data') else 'INACTIVE', resources='fetched'),
        DF.add_field('source', 'string', source_id, resources='fetched'),

        update_data_flow,

        test_hash(table_fields),

        DF.select_fields(
            ['id', 'source', 'status', *table_fields, AIRTABLE_ID_FIELD],
            resources='fetched'),

        dump_to_airtable({
            (airtable_base or settings.AIRTABLE_BASE, table): {
                'resource-name': 'fetched',
                'typecast': True
            }
        }, settings.AIRTABLE_API_KEY),
    )
