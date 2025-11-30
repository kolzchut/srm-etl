import pandas as pd
from conf import settings
from srm_tools.logger import logger
from extract.extract_data_from_airtable import load_airtable_as_dataframe


def get_branches_actual_id(df: pd.DataFrame) -> pd.DataFrame:
    """
       Reconciles the `branch_id` in the provided DataFrame with existing records in Airtable
       to prevent ID churn and duplicate records.

       Since `branch_id` is a hash generated from the address and organization, slight changes
       in data formatting (e.g., "St." vs "Street") can result in a new hash for the same physical branch.
       This function ensures that if a branch already exists in Airtable (matching Organization ID + Address),
       we use the *existing* ID instead of the newly calculated one.

       The process involves:
       1. **Organization Resolution**: Loads the 'Organizations' table to map internal Airtable
          Record IDs (e.g., `rec...`) to the actual Organization Business IDs (e.g., `5800...`).
       2. **Branch Lookup Map**: Loads the 'Branches' table and builds a dictionary mapping
          `(Organization_Business_ID, Address)` -> `Existing_Branch_ID`.
          - Only considers IDs starting with 'meser'.
          - Resolves linked organization fields using the map from step 1.
       3. **Reconciliation**: Iterates through the input DataFrame. If a row's Organization and Address
          match an entry in the lookup map, the local `branch_id` is overwritten with the
          existing Airtable ID.

       Args:
           df (pd.DataFrame): The local DataFrame containing calculated 'branch_id',
                              'organization_id', and 'address'.

       Returns:
           pd.DataFrame: The DataFrame with 'branch_id' updated to match Airtable where applicable.
       """
    logger.info("Starting ID Reconciliation: Fetching Tables...")

    try:
        orgs_df = load_airtable_as_dataframe(
            table_name="OrganizationsTest",
            base_id=settings.AIRTABLE_DATA_IMPORT_BASE
        )
        id_resolver = pd.Series(
            orgs_df['id'].values,
            index=orgs_df['_airtable_id']
        ).to_dict()

    except Exception as e:
        logger.error(f"Failed to load Organizations for reconciliation: {e}")
        return df

    try:
        branches_df = load_airtable_as_dataframe(
            table_name="BranchesTest",
            base_id=settings.AIRTABLE_DATA_IMPORT_BASE
        )
    except Exception as e:
        logger.error(f"Failed to load Branch table for reconciliation: {e}")
        return df

    existing_branches_map = {}

    for _, row in branches_df.iterrows():
        airtable_hash = str(row.get('id', ''))
        if not airtable_hash.startswith('meser'):
            continue

        address = str(row.get('address', '')).strip()
        if not address:
            continue

        org_links = row.get('organization')
        real_org_id = None

        if isinstance(org_links, list) and len(org_links) > 0:
            real_org_id = id_resolver.get(org_links[0])
        elif isinstance(org_links, str):
            real_org_id = id_resolver.get(org_links)

        if not real_org_id:
            continue

        key = (str(real_org_id).strip(), address)
        existing_branches_map[key] = airtable_hash

    logger.info(f"Built lookup map with {len(existing_branches_map)} valid 'meser' branches.")

    updated_count = 0

    def reconcile_row(row):
        nonlocal updated_count
        current_id = row['branch_id']

        df_org = str(row['organization_id']).strip() if pd.notna(row['organization_id']) else ''
        df_addr = str(row['address']).strip() if pd.notna(row['address']) else ''

        found_id = existing_branches_map.get((df_org, df_addr))

        if found_id and found_id != current_id:
            updated_count += 1
            return found_id

        return current_id

    if not df.empty:
        df['branch_id'] = df.apply(reconcile_row, axis=1)

    logger.info(f"ID Reconciliation: Updated {updated_count} branch IDs to match existing Airtable records.")
    return df
