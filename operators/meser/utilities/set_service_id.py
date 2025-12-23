import pandas as pd
from conf import settings
from extract.extract_data_from_airtable import load_airtable_as_dataframe
from srm_tools.hash import hasher
from srm_tools.logger import logger


def set_service_id(df: pd.DataFrame) -> pd.DataFrame:
    df['old_service_id'] = df.apply(
        lambda r: 'meser-' + hasher(r['service_name'], r['phone_numbers'], r['address'], r['organization_id'],
                                    r['branch_id']),
        axis=1
    )
    df['new_service_id'] = df.apply(
        lambda r: 'meser-s-' + r['meser_id'],
        axis=1
    )

    services_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE
    )

    existing_ids = set(services_df['id'].dropna())

    ## For log only
    counter_old = 0
    counter_new = 0

    def determine_id(row):
        nonlocal counter_old, counter_new
        if row['old_service_id'] in existing_ids:
            counter_old += 1
            return row['old_service_id']
        counter_new += 1
        return row['new_service_id']

    df['service_id'] = df.apply(determine_id, axis=1)

    df.drop(columns=['old_service_id', 'new_service_id'], inplace=True)

    logger.info(f"Services assigned old IDs: {counter_old}, new IDs: {counter_new}")
    return df
