import pandas as pd
from datetime import datetime

from pyairtable import Table

from load.airtable import get_airtable_table, update_airtable_records, create_airtable_records
from operators.meser_new.update import prepare_airtable_dataframe
from conf import settings
from srm_tools.logger import logger


def map_branches_ids_to_airtable_records(df: pd.DataFrame, branch_table_name: str, base_id: str) -> pd.DataFrame:
    """
    Replace branch_id values with Airtable record IDs.
    """
    branch_table: Table = get_airtable_table(branch_table_name, base_id)
    branch_map = {rec['fields']['id']: rec['id'] for rec in branch_table.all() if 'id' in rec['fields']}

    def replace_ids(branch_ids):
        if isinstance(branch_ids, list):
            return [branch_map.get(oid) for oid in branch_ids if oid in branch_map]
        elif branch_ids in branch_map:
            return [branch_map[branch_ids]]
        return []

    df['branches'] = df['branch_id'].apply(replace_ids)
    return df


def merge_branches(df: pd.DataFrame, existing_map: dict, airtable_key='id') -> pd.DataFrame:
    """
    Merge new branches IDs with existing ones to avoid overwriting.
    """
    def merge_row(row):
        service_id = row.get(airtable_key)
        new_branches = set(row.get('branches', []))
        existing_branches = existing_map.get(str(service_id), set())
        merged = list(existing_branches.union(new_branches))  # union avoids duplicates
        return merged

    df['branches'] = df.apply(merge_row, axis=1)
    return df

def get_existing_service_branches(table_name: str, base_id: str) -> dict:
    """
    Fetch existing branches links from Airtable for each service.
    Returns: {service_airtable_id: set of branches record IDs}
    """
    table = get_airtable_table(table_name, base_id)
    existing_map = {}
    for rec in table.all():
        branch_id = rec.get("fields", {}).get("id")
        orgs = rec.get("fields", {}).get("branches", [])
        if branch_id:
            existing_map[str(branch_id)] = set(orgs)
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
            'service_description': 'description'
        },
        inplace=True
    )
    df = enrich_service_fields(df)

    # Map branch IDs to Airtable record IDs
    df = map_branches_ids_to_airtable_records(df, "BranchesTest", settings.AIRTABLE_DATA_IMPORT_BASE)

    # Merge with existing branches to avoid overwriting
    existing_branches_map = get_existing_service_branches("ServicesTest", settings.AIRTABLE_DATA_IMPORT_BASE)
    df = merge_branches(df, existing_branches_map, airtable_key=airtable_key)

    # Prepare DataFrame for Airtable update
    fields_to_update = ['id', 'name', 'description', 'data_sources', 'situations', 'responses', 'branches', 'meser_id', 'source','status', 'decision']
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

