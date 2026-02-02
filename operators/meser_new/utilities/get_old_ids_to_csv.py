import pandas as pd
from conf import settings
from extract.extract_data_from_airtable import load_airtable_as_dataframe
from srm_tools.hash import hasher
from srm_tools.logger import logger


def get_old_ids_to_csv(df: pd.DataFrame):
    # 1. Calculate the 'old' IDs (hashes) inside the main dataframe
    df['old_branch_id'] = df.apply(
        lambda r: 'meser-' + hasher(r['address'], r['organization_id']), axis=1
    )
    df['old_service_id'] = df.apply(
        lambda r: 'meser-' + hasher(r['service_name'], r['phone_numbers'], r['address'], r['organization_id'],
                                    r['old_branch_id']), axis=1
    )

    # 2. Load the existing IDs from Airtable to verify matches
    services_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE
    )

    branchs_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_BRANCH_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE
    )

    # 3. Identify which generated IDs actually exist in Airtable
    #    (The 'id' column in Airtable corresponds to our 'old_branch_id')
    valid_branch_ids = set(branchs_df['id'].unique())
    valid_service_ids = set(services_df['id'].unique())

    # 4. Filter the original DF to find matches, then select the new IDs from DF

    # --- Branches ---
    matched_branches_df = df[df['old_branch_id'].isin(valid_branch_ids)].copy()

    # Select only the relevant columns from df and remove duplicates
    clean_matched_branches = matched_branches_df[['old_branch_id', 'branch_id']].drop_duplicates()

    # Rename for clarity if needed (optional, based on your preference)
    clean_matched_branches.rename(columns={'branch_id': 'new_branch_id'}, inplace=True)

    logger.info(f"Found {len(clean_matched_branches)} matching branches.")
    clean_matched_branches.to_csv('clean_matched_branches.csv', index=False)

    # --- Services ---
    matched_services_df = df[df['old_service_id'].isin(valid_service_ids)].copy()

    # Select only the relevant columns from df and remove duplicates
    clean_matched_services = matched_services_df[['old_service_id', 'service_id']].drop_duplicates()

    # Rename for clarity if needed
    clean_matched_services.rename(columns={'service_id': 'new_service_id'}, inplace=True)

    logger.info(f"Found {len(clean_matched_services)} matching services.")
    # Fixed filename here (was overwriting branches.csv)
    clean_matched_services.to_csv('clean_matched_services.csv', index=False)
