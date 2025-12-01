import pandas as pd
from conf import settings
from operators.meser_new.utilities.get_branches_actual_id import get_branches_actual_id
from utilities.update import prepare_airtable_dataframe
from operators.meser_new.utilities.get_foreign_key_by_field import get_foreign_key_by_field
from operators.meser_new.utilities.trigger_status_check import trigger_status_check
from srm_tools.logger import logger
from load.airtable import update_if_exists_if_not_create


def update_airtable_branches_from_df(df: pd.DataFrame) -> int:
    """
    Update or create Airtable branch records from Meser dataframe.
    Maps organization IDs to Airtable record IDs, merges with existing organizations,
    aggregates branches, enriches fields, and upserts to Airtable.
    """
    key_field = 'branch_id'
    airtable_key = 'id'
    fields_to_update = [
        'branch_id', 'name', 'organization', 'address',
        'phone_numbers', 'source','status'
    ]

    # Prepare branch data
    df['name'] = df['branch_name']
    df['organization'] = df['organization_id']

    # Map organization IDs to Airtable record IDs and merge with existing ones
    df = get_foreign_key_by_field(
        df=df,
        current_table=settings.AIRTABLE_BRANCH_TABLE,
        source_table=settings.AIRTABLE_ORGANIZATION_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        base_field="organization_id",
        target_field="organization",
        airtable_key=airtable_key
    )

    # Aggregate branches
    df = df.groupby('branch_id').agg({
        'organization': lambda x: list({oid for sublist in x for oid in sublist if oid}),
        'name': 'first',
        'address': 'first',
        'phone_numbers': lambda x: ', '.join(
            [str(v) for v in x if pd.notna(v) and str(v).strip() not in ('', '0')])
    }).reset_index()

    # Enrich fields
    df['source'] = 'meser'
    trigger_status_check(df=df, table_name=settings.AIRTABLE_BRANCH_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                         airtable_key_field='id', active_value='ACTIVE', inactive_value='INACTIVE', only_from_source='meser', df_key_field='branch_id', batch_size=50)
    df['status'] = 'ACTIVE'

    # Prepare DataFrame for Airtable
    df_prepared = prepare_airtable_dataframe(df, key_field, fields_to_update, airtable_key)

    if df_prepared.empty:
        logger.info("No branch records to update.")
        return 0

    # Upsert to Airtable
    return update_if_exists_if_not_create(
        df=df_prepared,
        table_name=settings.AIRTABLE_BRANCH_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        airtable_key=airtable_key
    )
