from typing import Tuple
from srm_tools.logger import logger

from extract.extract_data_from_airtable import load_airtable_as_dataframe
from conf import settings
import pandas as pd

from load.airtable import update_if_exists_if_not_create, update_airtable_records
from utilities.update import prepare_airtable_dataframe


def fix_services_data(services_df: pd.DataFrame):
    services_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_STAGING_BASE
    )
    services_airtable_df['isChanged'] = False
    fields_to_copy = ['Cards']
    def copy_values(row):
        if row['id'] in services_df['new_service_id'].values:
            airtable_new_id = services_df.loc[services_df['new_service_id'] == row['id'], 'old_service_id'].values[0]
            for field in fields_to_copy:
                if field in services_airtable_df.columns:
                    row[field] = services_airtable_df.loc[services_airtable_df['id'] == airtable_new_id, field].values[0]
            row['isChanged'] = True
        return row
    services_airtable_df = services_airtable_df.apply(copy_values, axis=1)
    services_airtable_df = services_airtable_df[services_airtable_df['isChanged']]
    prepared_services_df = prepare_airtable_dataframe(df=services_airtable_df, key_field="id", airtable_key="id", fields_to_prepare=fields_to_copy)
    prepared_services_df = prepared_services_df.where(pd.notnull(prepared_services_df), None)
    if prepared_services_df.empty:
        return 0
    return update_airtable_records(
        df=prepared_services_df,
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_STAGING_BASE,
        key_field="id",
        retrieve_not_updated_ids=True
    )

def fix_branches_data(branches_df: pd.DataFrame):
    branches_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_BRANCH_TABLE,
        base_id=settings.AIRTABLE_STAGING_BASE
    )
    branches_airtable_df['isChanged'] = False
    fields_to_copy = ['Cards']
    def copy_values(row):
        if row['id'] in branches_df['new_branch_id'].values:
            airtable_new_id = branches_df.loc[branches_df['new_branch_id'] == row['id'], 'old_branch_id'].values[0]
            for field in fields_to_copy:
                if field in branches_airtable_df.columns:
                    row[field] = branches_airtable_df.loc[branches_airtable_df['id'] == airtable_new_id, field].values[0]
            row['isChanged'] = True
        return row
    branches_airtable_df = branches_airtable_df.apply(copy_values, axis=1)
    branches_airtable_df = branches_airtable_df[branches_airtable_df['isChanged']]
    prepared_branches_df = prepare_airtable_dataframe(df=branches_airtable_df, key_field="id", airtable_key="id", fields_to_prepare=fields_to_copy)
    prepared_branches_df = prepared_branches_df.where(pd.notnull(prepared_branches_df), None)
    if prepared_branches_df.empty:
        return 0
    return update_airtable_records(
        df=prepared_branches_df,
        table_name=settings.AIRTABLE_BRANCH_TABLE,
        base_id=settings.AIRTABLE_STAGING_BASE,
        key_field="id",
        retrieve_not_updated_ids=True
    )



def get_matching_dataframes() ->Tuple[pd.DataFrame, pd.DataFrame]:

    branches_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_BRANCH_TABLE,
        base_id=settings.AIRTABLE_STAGING_BASE
    )
    services_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_STAGING_BASE
    )

    branches_df = pd.read_csv('clean_matched_branches.csv')
    services_df = pd.read_csv('clean_matched_services.csv')

    airtable_branch_map = branches_airtable_df.drop_duplicates(subset=['id']).set_index('id')['_airtable_id']

    branches_df['new_branch_airtable_id'] = branches_df['new_branch_id'].map(airtable_branch_map)
    branches_df['old_branch_airtable_id'] = branches_df['old_branch_id'].map(airtable_branch_map)

    airtable_service_map = services_airtable_df.drop_duplicates(subset=['id']).set_index('id')['_airtable_id']
    services_df['new_service_airtable_id'] = services_df['new_service_id'].map(airtable_service_map)
    services_df['old_service_airtable_id'] = services_df['old_service_id'].map(airtable_service_map)

    return branches_df, services_df


def run(*_):
    logger.info("This operator is one time run to transform old IDs to new IDs. Working on Manual Fixes, Branches and Services Tables")
    branches_df, services_df = get_matching_dataframes()
    services_df.dropna(inplace=True)
    branches_df.dropna(inplace=True)

    fix_services_data(services_df=services_df)
    fix_branches_data(branches_df=branches_df)


if __name__ == '__main__':
    run(None, None, None)
