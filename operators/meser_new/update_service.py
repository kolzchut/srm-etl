import pandas as pd
from datetime import datetime

from pyairtable import Table

from load.airtable import get_airtable_table, update_airtable_records, create_airtable_records
from operators.meser_new.update import prepare_airtable_dataframe
from conf import settings
from srm_tools.logger import logger


def map_df_values_to_airtable_record_ids(
    df: pd.DataFrame,
    table_name: str,
    base_id: str,
    target_field_in_df: str,
    base_field_in_df: str,
    airtable_key_field: str = "id"
) -> pd.DataFrame:
    """
    Map values from a DataFrame column to Airtable record IDs based on a key field.

    Args:
        df (pd.DataFrame): The DataFrame containing the values to map.
        table_name (str): Airtable table name.
        base_id (str): Airtable base ID.
        target_field_in_df (str): Column in DataFrame to store the mapped record IDs.
        base_field_in_df (str): Column in DataFrame containing the original values to match.
        airtable_key_field (str): Field in Airtable used as the lookup key (default: 'id').

    Returns:
        pd.DataFrame: DataFrame with the target column updated to contain Airtable record IDs.
    """
    table: Table = get_airtable_table(table_name, base_id)
    record_map = {
        rec["fields"][airtable_key_field]: rec["id"]
        for rec in table.all()
        if airtable_key_field in rec.get("fields", {})
    }

    def replace_values(value):
        if isinstance(value, list):
            return [record_map.get(v) for v in value if v in record_map]
        elif value in record_map:
            return [record_map[value]]
        return []

    df[target_field_in_df] = df[base_field_in_df].apply(replace_values)
    return df


def merge_foreign_key(df: pd.DataFrame, existing_map: dict, field_name:str, airtable_key='id') -> pd.DataFrame:
    """
    Merge new foreign keys with existing ones to avoid overwriting.
    """
    def merge_row(row):
        service_id = row.get(airtable_key)
        new_branches = set(row.get(field_name, []))
        existing_branches = existing_map.get(str(service_id), set())
        merged = list(existing_branches.union(new_branches))  # union avoids duplicates
        return merged

    df[field_name] = df.apply(merge_row, axis=1)
    return df

def get_existing_field_links(
    table_name: str,
    base_id: str,
    linked_field: str,
    key_field: str = "id"
) -> dict:
    """
    Fetch links between two Airtable fields in a table..

    Args:
        linked_field (str): Field name to use as the linked list (e.g., 'branches').
        table_name (str): Airtable table name.
        base_id (str): Airtable base ID.
        key_field (str): Field name to use as the key (default: 'id').

    Returns:
        dict: {key_field_value: set(linked_field_values)}
    """
    table = get_airtable_table(table_name, base_id)
    existing_map = {}

    for rec in table.all():
        key = rec.get("fields", {}).get(key_field)
        linked_values = rec.get("fields", {}).get(linked_field, [])
        if key:
            existing_map[str(key)] = set(linked_values)

    return existing_map


def enrich_service_fields(df: pd.DataFrame) -> pd.DataFrame:
    df['source'] = 'meser'
    df['status'] = 'ACTIVE'
    df['decision'] = "New"
    df['data_sources'] = "מידע על מסגרות רווחה התקבל ממשרד הרווחה והשירותים החברתיים"

    return df


def update_airtable_services_from_df(df: pd.DataFrame) -> int:
    key_field = 'id'
    airtable_key = 'id'

    df.rename(
        columns={
            'service_id': 'id',
            'service_name': 'name',
        },
        inplace=True
    )
    df = enrich_service_fields(df)

    branches_id_field_name_in_services_table = 'branches'

    # Map branch IDs to Airtable record IDs
    df = map_df_values_to_airtable_record_ids(df=df, table_name="BranchesTest", base_id=settings.AIRTABLE_DATA_IMPORT_BASE, target_field_in_df="branches", base_field_in_df="branch_id", airtable_key_field=airtable_key)

    # Merge with existing branches to avoid overwriting
    existing_branches_map = get_existing_field_links(table_name="ServicesTest", base_id=settings.AIRTABLE_DATA_IMPORT_BASE, linked_field=branches_id_field_name_in_services_table, key_field=airtable_key)
    df = merge_foreign_key(df=df, existing_map=existing_branches_map, airtable_key=airtable_key, field_name=branches_id_field_name_in_services_table)

    # Prepare DataFrame for Airtable update
    fields_to_update = ['id', 'name', 'data_sources', 'situations', 'responses', 'branches', 'meser_id', 'source','status', 'decision']
    df = prepare_airtable_dataframe(df, key_field, fields_to_update, airtable_key)

    if df.empty:
        logger.info("No service records to update.")
        return 0

    # Update existing records
    modified_count, not_found = update_airtable_records(
        df=df,
        table_name="ServicesTest",
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        key_field=airtable_key,
        batch_size=50,
        retrieve_not_updated_ids=True
    )
    logger.info(f"Updated {modified_count} service records in Airtable")

    # Create missing records
    if not_found:
        df_to_create = df[df[airtable_key].isin(not_found)]
        created_count = create_airtable_records(
            df=df_to_create,
            table_name="ServicesTest",
            base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
            batch_size=50
        )
        logger.info(f"Created {created_count} new service records in Airtable")
        modified_count += created_count

    return modified_count

