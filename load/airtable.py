from typing import List, Dict, Any, cast, Optional
import pandas as pd
from pyairtable import Api
from pyairtable.api.table import Table, UpdateRecordDict
from srm_tools.logger import logger
from conf import settings


def get_airtable_table(table_name: str, base_id: str) -> Table:
    """Return an Airtable Table object."""
    api = Api(settings.AIRTABLE_API_KEY)
    return api.table(base_id, table_name)


def should_update_record(new_fields: Dict[str, Any], old_fields: Dict[str, Any]) -> bool:
    """
    Check if the record should be updated.
    If the only field that changed is 'source', do not update.
    """
    changes = []
    for key, new_val in new_fields.items():
        old_val = old_fields.get(key)

        if new_val == old_val:
            continue

        if str(new_val) == str(old_val):
            continue

        changes.append(key)

    if not changes:
        return False

    if len(changes) == 1 and changes[0] == 'source':
        return False

    return True


def update_airtable_records(
    df: pd.DataFrame,
    table_name: str,
    base_id: str,
    key_field: str,
    batch_size: int = 10,
    retrieve_not_updated_ids: bool = False
) -> tuple[int, set[Any]] | int:
    """
    Update existing Airtable records using DataFrame columns as field names.

    :param df: DataFrame with data to update
    :param table_name: Airtable table name
    :param base_id: Airtable base ID
    :param key_field: Airtable field used to match records
    :param batch_size: Number of records to update per batch
    :param retrieve_not_updated_ids: Whether to return IDs of records not found in Airtable
    :return: Number of modified records
    """
    table = get_airtable_table(table_name, base_id)
    # Build map: key_field -> record
    record_map: Dict[str, Any] = {}
    for record in table.all():
        val = record.get("fields", {}).get(key_field)
        if isinstance(val, list) and val:
            val = val[0]
        if isinstance(val, str):
            val = val.strip()
            if val:
                record_map[val] = record

    updates: List[UpdateRecordDict] = []
    not_found = set()

    for _, row in df.iterrows():
        key_val = row.get(key_field)
        if not key_val:
            continue

        airtable_record = record_map.get(str(key_val).strip())
        if airtable_record:
            airtable_rec_id = airtable_record["id"]
            existing_fields = airtable_record.get("fields", {})

            fields: Dict[str, Any] = {
                col: row[col]
                for col in df.columns
                if col != key_field and row.get(col) not in (None, '', 0, 'None')
            }

            if should_update_record(fields, existing_fields):
                updates.append({"id": airtable_rec_id, "fields": fields})
        else:
            not_found.add(str(key_val))

    if not_found:
        logger.warning(f"Records not found in Airtable (not updated): {', '.join(sorted(not_found))}")

    modified_count = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        try:
            table.batch_update(cast(List[UpdateRecordDict], batch))
            modified_count += len(batch)
        except Exception as e:
            logger.error(f"Failed batch update: {e}")

    logger.info(f"Finished updating Airtable. Total modified: {modified_count}")
    if retrieve_not_updated_ids:
        return modified_count, not_found
    return modified_count


def create_airtable_records(
    df: pd.DataFrame,
    table_name: str,
    base_id: str,
    batch_size: int = 10
) -> int:
    """
    Create new Airtable records using DataFrame columns as field names.

    :param df: DataFrame with data to create
    :param table_name: Airtable table name
    :param base_id: Airtable base ID
    :param batch_size: Number of records to create per batch
    :return: Number of created records
    """
    table = get_airtable_table(table_name, base_id)

    records: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        fields: Dict[str, Any] = {col: row[col] for col in df.columns if row.get(col) is not None}
        if fields:
            records.append(fields)

    created_count = 0
    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        try:
            table.batch_create(cast(List[Dict[str, Any]], batch))
            created_count += len(batch)
        except Exception as e:
            logger.error(f"Failed batch create: {e}")

    logger.info(f"Finished creating Airtable records. Total created: {created_count}")
    return created_count


def update_if_exists_if_not_create(
        df: pd.DataFrame,
        table_name: str,
        base_id: str,
        airtable_key: str,
        batch_size: int = 50,
        fields_to_update: Optional[List[str]] = None,
        fields_to_create: Optional[List[str]] = None
) -> int:
    """
    Update existing Airtable records or create new ones if they don't exist.

    :param df: DataFrame with data to update or create
    :param table_name: Airtable table name
    :param base_id: Airtable base ID
    :param airtable_key: Airtable field used to match records
    :param batch_size: Number of records to process per batch
    :param fields_to_update: Specific list of columns to update. If None, all columns are updated.
    :param fields_to_create: Specific list of columns to create. If None, all columns are used for creation.
    :return: Total number of modified or created records
    """
    df_for_update = df.copy()
    if fields_to_update is not None:
        update_cols = list(set(fields_to_update) | {airtable_key})
        valid_update_cols = [c for c in update_cols if c in df.columns]
        df_for_update = df[valid_update_cols].copy()

    modified_count, not_found = update_airtable_records(
        df=df_for_update,
        table_name=table_name,
        base_id=base_id,
        key_field=airtable_key,
        batch_size=batch_size,
        retrieve_not_updated_ids=True
    )
    logger.info(f"Updated {modified_count} branch records in Airtable")

    if not_found:
        df_for_create = df[df[airtable_key].isin(not_found)].copy()

        if fields_to_create is not None:
            create_cols = list(set(fields_to_create) | {airtable_key})
            valid_create_cols = [c for c in create_cols if c in df_for_create.columns]
            df_for_create = df_for_create[valid_create_cols]

        created_count = create_airtable_records(
            df=df_for_create,
            table_name=table_name,
            base_id=base_id,
            batch_size=batch_size
        )
        logger.info(f"Created {created_count} new branch records in Airtable")
        modified_count += created_count

    return modified_count
