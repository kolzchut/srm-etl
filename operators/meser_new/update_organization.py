import pandas as pd

from operators.meser_new.update import prepare_airtable_dataframe
from srm_tools.logger import logger
from conf import settings
from load.airtable import update_airtable_records, create_airtable_records


def clean_fields_for_organization_airtable(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare fields to be compatible with organization table."""
    if 'situations' in df.columns:
        # If it's a list, join with commas; otherwise leave as string
        df['situations'] = df['situations'].apply(lambda x: ', '.join(x) if isinstance(x, list) else str(x) if x else '')
    return df


def update_airtable_organizations_from_df(df: pd.DataFrame) -> int:
    """
    Update Airtable organizations table from Meser dataframe.
    Only updates specified fields using 'organization_id' as key.
    """
    key_field = 'organization_id'
    airtable_key = 'id'
    fields_to_update = [
        'organization_id', 'situations', 'phone_numbers', 'meser_id'
    ]

    df = clean_fields_for_organization_airtable(df)

    if key_field not in df.columns:
        logger.warning(f"DataFrame does not contain '{key_field}' column.")
        return 0

    df_for_update = prepare_airtable_dataframe(df, key_field, fields_to_update, airtable_key)

    if df_for_update.empty:
        logger.info("No records to update.")
        return 0

    # Update Airtable
    modified_count, not_found = update_airtable_records(
        df=df_for_update,
        table_name="OrganizationsTest",
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        key_field=airtable_key,
        batch_size=50,
        retrieve_not_updated_ids=True
    )

    # Create missing records
    if not_found:
        df_to_create = df_for_update[df_for_update[airtable_key].isin(not_found)]
        created_count = create_airtable_records(
            df=df_to_create,
            table_name="OrganizationsTest",
            base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
            batch_size=50
        )
        logger.info(f"Created {created_count} new organization records in Airtable")
        modified_count += created_count

    return modified_count



