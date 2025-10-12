import pandas as pd

from operators.meser_new.update import prepare_airtable_dataframe
from operators.meser_new.utilities.trigger_status_check import trigger_status_check
from srm_tools.logger import logger
from conf import settings
from load.airtable import update_airtable_records, create_airtable_records, update_if_exists_if_not_create


def clean_fields_for_organization_airtable(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare fields to be compatible with organization table."""
    if 'situations' in df.columns:
        # If it's a list, join with commas; otherwise leave as string
        df['situations'] = df['situations'].apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x) if x else '')
    return df


def update_airtable_organizations_from_df(df: pd.DataFrame) -> int:
    key_field = 'organization_id'
    airtable_key = 'id'
    fields_to_update = ['organization_id', 'situations', 'phone_numbers', 'meser_id','source']

    df['source'] = 'meser'
    trigger_status_check(df=df, table_name='OrganizationsTest', base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                         airtable_key_field='id', active_value='ACTIVE', inactive_value='INACTIVE', only_from_source='meser', df_key_field='organization_id', batch_size=50)

    df = clean_fields_for_organization_airtable(df)

    if key_field not in df.columns:
        logger.warning(f"DataFrame does not contain '{key_field}' column.")
        return 0

    df_prepared = prepare_airtable_dataframe(df, key_field, fields_to_update, airtable_key)

    if df_prepared.empty:
        logger.info("No organization records to update.")
        return 0

    return update_if_exists_if_not_create(
        df=df_prepared,
        table_name="OrganizationsTest",
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        airtable_key=airtable_key
    )



