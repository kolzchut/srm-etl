import pandas as pd
from pyairtable import Table
from conf import settings
from load.airtable import get_airtable_table


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

def get_foreign_key_by_field (
    df: pd.DataFrame,
    current_table: str,
    source_table: str,
    base_id: str,
    base_field: str,
    target_field: str,
    airtable_key: str = "id"
) -> pd.DataFrame:
    # Map branch IDs to Airtable record IDs
    df = map_df_values_to_airtable_record_ids(df=df, table_name=source_table,
                                              base_id=base_id, target_field_in_df=target_field,
                                              base_field_in_df=base_field, airtable_key_field=airtable_key)

    # Merge with existing branches to avoid overwriting
    existing_branches_map = get_existing_field_links(table_name=current_table,
                                                     base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                                                     linked_field=target_field,
                                                     key_field=airtable_key)
    df = merge_foreign_key(df=df, existing_map=existing_branches_map, airtable_key=airtable_key,
                           field_name=target_field)
    return df
