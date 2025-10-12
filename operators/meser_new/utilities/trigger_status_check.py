from typing import Dict, Any
import pandas as pd
from load.airtable import update_airtable_records, get_airtable_table
from operators.meser_new.update import prepare_airtable_dataframe


def fetch_airtable_records(table_name: str, base_id: str, airtable_key_field: str, only_from_source: str = "") -> Dict[str, Dict[str, Any]]:
    """
    Fetch all Airtable records and build a mapping of key_field -> record info.

    Parameters
    ----------
    table_name : str
        Name of the Airtable table.
    base_id : str
        Airtable base ID.
    airtable_key_field : str
        The Airtable field used to identify records.
    only_from_source : str, optional
        If provided, only include records with this source value.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Mapping of key_field values to record information including 'id' and current 'status'.
    """
    table = get_airtable_table(table_name, base_id)
    records = table.all()
    record_map: Dict[str, Dict[str, Any]] = {}

    for record in records:
        fields = record.get("fields", {})
        val = fields.get(airtable_key_field)
        if isinstance(val, list) and val:
            val = val[0]
        if isinstance(val, str):
            val = val.strip()
            if val and (not only_from_source or fields.get("source") == only_from_source):
                record_map[val] = {"id": record["id"], "status": fields.get("status")}
    return record_map


def build_status_update_dataframe(df: pd.DataFrame, df_key_field: str, record_map: Dict[str, Dict[str, Any]],
                                  active_value: str, inactive_value: str) -> pd.DataFrame:
    """
    Create a DataFrame containing Airtable records with updated status.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing IDs that should be active.
    df_key_field : str
        Column in df to match Airtable records.
    record_map : Dict[str, Dict[str, Any]]
        Mapping of Airtable records keyed by key_field.
    active_value : str
        Status value to assign to active records.
    inactive_value : str
        Status value to assign to inactive records.

    Returns
    -------
    pd.DataFrame
        DataFrame with key_field and updated status for records whose status changed.
    """
    existing_ids = set(df[df_key_field].dropna().astype(str))
    update_data = []

    for key_val, rec in record_map.items():
        new_status = active_value if key_val in existing_ids else inactive_value
        if rec["status"] != new_status:
            update_data.append({df_key_field: key_val, "status": new_status})

    return pd.DataFrame(update_data)


def trigger_status_check(
    df: pd.DataFrame,
    table_name: str,
    base_id: str,
    airtable_key_field: str,
    df_key_field: str = "id",
    active_value: str = "Active",
    inactive_value: str = "Inactive",
    only_from_source: str = "",
    batch_size: int = 10
) -> int:
    """
    Synchronize the 'status' field in Airtable with a DataFrame.

    For each record in the Airtable table:
    - If the value in `df_key_field` exists in `df`, sets 'status' to `active_value`.
    - If it does not exist in `df`, sets 'status' to `inactive_value`.
    - Optionally filters Airtable records by `source` if `only_from_source` is provided.

    Only records whose status has changed are updated. The updates are performed in batches
    using `update_airtable_records`.

    Parameters
    ----------
    df : pd.DataFrame
        DataFrame containing IDs that should be marked as active.
    table_name : str
        Airtable table name.
    base_id : str
        Airtable base ID.
    airtable_key_field : str
        The field in Airtable used to match records.
    df_key_field : str, default "id"
        Column in df containing the IDs.
    active_value : str, default "Active"
        Value to set for active records.
    inactive_value : str, default "Inactive"
        Value to set for inactive records.
    only_from_source : str, default ""
        If provided, only update records with this source.
    batch_size : int, default 10
        Number of records to update per batch.

    Returns
    -------
    int
        Total number of records updated in Airtable.
    """
    record_map = fetch_airtable_records(table_name, base_id, airtable_key_field, only_from_source)
    if not record_map:
        return 0

    update_df = build_status_update_dataframe(df, df_key_field, record_map, active_value, inactive_value)
    if update_df.empty:
        return 0

    update_df = prepare_airtable_dataframe(df=update_df, key_field=df_key_field, fields_to_update=["status"],
                                           airtable_key=airtable_key_field)

    modified_count = update_airtable_records(
        df=update_df,
        table_name=table_name,
        base_id=base_id,
        key_field=airtable_key_field,
        batch_size=batch_size
    )

    return modified_count
