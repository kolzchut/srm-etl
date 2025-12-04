import pandas as pd
from conf import settings
from srm_tools.logger import logger
from extract.extract_data_from_airtable import load_airtable_as_dataframe


def get_services_actual_id(df: pd.DataFrame) -> pd.DataFrame:
    """
       Reconciles the `service_id` in the provided DataFrame with existing records in Airtable
       to prevent ID churn and duplicate records.

       Since `service_id` is a hash generated from multiple fields, slight changes
       in data formatting can result in a new hash for the same service.
       This function ensures that if a service already exists in Airtable (matching Service Name + at least one situation),
       we use the *existing* ID instead of the newly calculated one.

       The process involves:
       1. **Service Lookup Map**: Loads the settings.AIRTABLE_SERVICE_TABLE table and builds a dictionary mapping
          `Service_Name` -> list of `(situations_set, Existing_Service_ID)`.
          - Only considers IDs starting with 'meser'.
       2. **Reconciliation**: Iterates through the input DataFrame. If a row's Service Name matches
          and at least one situation overlaps with an entry in the lookup map, the local `service_id`
          is overwritten with the existing Airtable ID.

       Args:
           df (pd.DataFrame): The local DataFrame containing calculated 'service_id',
                              'service_name', and 'situations'.

       Returns:
           pd.DataFrame: The DataFrame with 'service_id' updated to match Airtable where applicable.
       """
    logger.info("Starting Service ID Reconciliation: Fetching Services Table...")

    try:
        services_df = load_airtable_as_dataframe(
            table_name=settings.AIRTABLE_SERVICE_TABLE,
            base_id=settings.AIRTABLE_DATA_IMPORT_BASE
        )
    except Exception as e:
        logger.error(f"Failed to load settings.AIRTABLE_SERVICE_TABLE table for reconciliation: {e}")
        return df

    # Map: service_name -> list of (situations_set, service_id)
    existing_services_map = {}

    for _, row in services_df.iterrows():
        airtable_hash = str(row.get('id', ''))
        if not airtable_hash.startswith('meser'):
            continue

        service_name = str(row.get('name', '')).strip()
        situations_raw = row.get('situations', [])

        # Handle situations as array, normalize to set for intersection checking
        if isinstance(situations_raw, list):
            situations = set([str(s).strip() for s in situations_raw if s])
        else:
            situations = set()

        if not service_name or not situations:
            continue

        if service_name not in existing_services_map:
            existing_services_map[service_name] = []

        existing_services_map[service_name].append((situations, airtable_hash))

    logger.info(f"Built lookup map with {sum(len(v) for v in existing_services_map.values())} valid 'meser' services.")

    updated_count = 0

    def reconcile_row(row):
        nonlocal updated_count
        current_id = row['service_id']

        df_service_name = str(row['service_name']).strip() if pd.notna(row['service_name']) else ''

        situations_raw = row['situations']
        if isinstance(situations_raw, list):
            df_situations = set([str(s).strip() for s in situations_raw if s])
        else:
            df_situations = set()

        if df_service_name in existing_services_map:
            for existing_situations, existing_id in existing_services_map[df_service_name]:
                if df_situations & existing_situations:
                    if existing_id != current_id:
                        updated_count += 1
                        return existing_id
                    break

        return current_id

    if not df.empty:
        df['service_id'] = df.apply(reconcile_row, axis=1)

    logger.info(f"Service ID Reconciliation: Updated {updated_count} service IDs to match existing Airtable records.")
    return df
