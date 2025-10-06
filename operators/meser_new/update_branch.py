from datetime import datetime
import pandas as pd
from pyairtable import Table
from conf import settings
from operators.meser_new.update import prepare_airtable_dataframe
from srm_tools.logger import logger
from load.airtable import update_airtable_records, create_airtable_records, get_airtable_table


def map_organization_ids_to_airtable_records(df: pd.DataFrame, org_table_name: str, base_id: str) -> pd.DataFrame:
    """
    Replace organization_id values with Airtable record IDs.
    """
    org_table: Table = get_airtable_table(org_table_name, base_id)
    org_map = {rec['fields']['id']: rec['id'] for rec in org_table.all() if 'id' in rec['fields']}

    def replace_ids(org_ids):
        if isinstance(org_ids, list):
            return [org_map.get(oid) for oid in org_ids if oid in org_map]
        elif org_ids in org_map:
            return [org_map[org_ids]]
        return []

    df['organization'] = df['organization_id'].apply(replace_ids)
    return df


def get_existing_branch_organizations(table_name: str, base_id: str) -> dict:
    """
    Fetch existing organization links from Airtable for each branch.
    Returns: {branch_airtable_id: set of organization record IDs}
    """
    table = get_airtable_table(table_name, base_id)
    existing_map = {}
    for rec in table.all():
        branch_id = rec.get("fields", {}).get("id")
        orgs = rec.get("fields", {}).get("organization", [])
        if branch_id:
            existing_map[str(branch_id)] = set(orgs)
    return existing_map


def merge_organizations(df: pd.DataFrame, existing_map: dict, airtable_key='id') -> pd.DataFrame:
    """
    Merge new organization IDs with existing ones to avoid overwriting.
    """
    def merge_row(row):
        branch_id = row.get(airtable_key)
        new_orgs = set(row.get('organization', []))
        existing_orgs = existing_map.get(str(branch_id), set())
        merged = list(existing_orgs.union(new_orgs))  # union avoids duplicates
        return merged

    df['organization'] = df.apply(merge_row, axis=1)
    return df


def aggregate_branches(df: pd.DataFrame) -> pd.DataFrame:
    """
    Group branches by branch_id and aggregate fields.
    """
    return df.groupby('branch_id').agg({
        'organization': lambda x: list({oid for sublist in x for oid in sublist if oid}),  # flatten + remove duplicates
        'name': 'first',
        'address': 'first',
        'phone_numbers': lambda x: ', '.join(
            [str(v) for v in x if pd.notna(v) and str(v).strip() not in ('', '0')])
    }).reset_index()


def enrich_branch_fields(df: pd.DataFrame) -> pd.DataFrame:
    df['source'] = 'meser'
    df['status'] = 'ACTIVE'
    return df


def update_airtable_branches_from_df(df: pd.DataFrame) -> int:
    """
    Update or create Airtable branch records from Meser dataframe.
    Ensures organizations are merged instead of overwritten.
    """
    key_field = 'branch_id'
    airtable_key = 'id'
    fields_to_update = ['branch_id', 'name', 'organization', 'address', 'phone_numbers',
                        'source', 'status']

    df['name'] = df['branch_name']
    df['organization'] = df['organization_id']

    # Replace organization IDs with Airtable record IDs
    df = map_organization_ids_to_airtable_records(df, org_table_name="OrganizationsTest",
                                                  base_id=settings.AIRTABLE_DATA_IMPORT_BASE)

    # Aggregate branches
    df = aggregate_branches(df)

    # Fetch existing linked organizations to merge
    existing_orgs_map = get_existing_branch_organizations("BranchesTest", settings.AIRTABLE_DATA_IMPORT_BASE)
    df = merge_organizations(df, existing_orgs_map, airtable_key=airtable_key)

    # Enrich fields
    df = enrich_branch_fields(df)

    # Prepare for Airtable
    df_for_update = prepare_airtable_dataframe(df, key_field, fields_to_update, airtable_key)

    if df_for_update.empty:
        logger.info("No branch records to update.")
        return 0

    # Update existing records
    modified_count, not_found = update_airtable_records(
        df=df_for_update,
        table_name="BranchesTest",
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        key_field=airtable_key,
        batch_size=50,
        retrieve_not_updated_ids=True
    )
    logger.info(f"Updated {modified_count} branch records in Airtable")

    # Create missing records
    if not_found:
        df_to_create = df_for_update[df_for_update[airtable_key].isin(not_found)]
        created_count = create_airtable_records(
            df=df_to_create,
            table_name="BranchesTest",
            base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
            batch_size=50
        )
        logger.info(f"Created {created_count} new branch records in Airtable")
        modified_count += created_count

    return modified_count
