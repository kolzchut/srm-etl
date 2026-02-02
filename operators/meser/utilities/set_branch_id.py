import pandas as pd
from conf import settings
from extract.extract_data_from_airtable import load_airtable_as_dataframe
from srm_tools.hash import hasher
from srm_tools.logger import logger


def set_branch_id(df: pd.DataFrame) -> pd.DataFrame:
    df['old_branch_id'] = df.apply(lambda r: 'meser-' + hasher(r['address'], r['organization_id']), axis=1)

    df['new_branch_id'] = df.apply(
        lambda r: 'meser-b-' + r['meser_id'],
        axis=1
    )

    branchs_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_BRANCH_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE
    )

    existing_ids = set(branchs_df['id'].dropna())

    ## For log only
    counter_old = 0
    counter_new = 0

    def determine_id(row):
        nonlocal counter_old, counter_new
        if row['old_branch_id'] in existing_ids:
            counter_old += 1
            return row['old_branch_id']
        counter_new += 1
        return row['new_branch_id']

    df['branch_id'] = df.apply(determine_id, axis=1)

    df.drop(columns=['old_branch_id', 'new_branch_id'], inplace=True)
    logger.info(f"Branches assigned old IDs: {counter_old}, new IDs: {counter_new}")

    return df
