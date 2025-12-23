from typing import Tuple
from srm_tools.logger import logger

from extract.extract_data_from_airtable import load_airtable_as_dataframe
from conf import settings
import pandas as pd

from load.airtable import update_if_exists_if_not_create, update_airtable_records
from utilities.update import prepare_airtable_dataframe


def fix_manual_data(branches_df: pd.DataFrame, services_df: pd.DataFrame):
    manual_fixes_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_MANUAL_FIXES_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE
    )
    manual_fixes_airtable_df['isChanged'] = False
    def replace_ids(row):
        # Check if 'Branches' is actually a list before iterating
        if isinstance(row['Branches'], list):
            for val in row['Branches']:
                if val in branches_df['old_branch_airtable_id'].values:
                    # Perform the replacement logic
                    row['Branches'] = [
                        branches_df.loc[branches_df['old_branch_airtable_id'] == val, 'new_branch_airtable_id'].values[
                            0]
                        if b == val else b
                        for b in row['Branches']
                    ]
                    row['isChanged'] = True

        # Check if 'Services' is actually a list before iterating
        if isinstance(row['Services'], list):
            for val in row['Services']:
                if val in services_df['old_service_airtable_id'].values:
                    # Perform the replacement logic
                    row['Services'] = [
                        services_df.loc[
                            services_df['old_service_airtable_id'] == val, 'new_service_airtable_id'].values[0]
                        if s == val else s
                        for s in row['Services']
                    ]
                    row['isChanged'] = True
        return row
    manual_fixes_airtable_df = manual_fixes_airtable_df.apply(replace_ids, axis=1)
    manual_fixes_airtable_df = manual_fixes_airtable_df[manual_fixes_airtable_df['isChanged']]
    prepared_manual_fixes_df = prepare_airtable_dataframe(df=manual_fixes_airtable_df, key_field="identifier", airtable_key="identifier", fields_to_update=["Branches", "Services"])
    prepared_manual_fixes_df = prepared_manual_fixes_df.where(pd.notnull(prepared_manual_fixes_df), None)
    if prepared_manual_fixes_df.empty:
        return 0

    return update_airtable_records(
        df=prepared_manual_fixes_df,
        table_name=settings.AIRTABLE_MANUAL_FIXES_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        key_field="identifier",
        retrieve_not_updated_ids=True
    )
def fix_services_data(services_df: pd.DataFrame):
    services_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE
    )
    services_airtable_df['isChanged'] = False
    fields_to_copy = ['decision','reason','# boost', 'עבר בדיקה - מצבים','עבר כתיבת טקסט', 'עבר בדיקה - מענים','עבר טיוב גיל רך', 'עבר בדיקה - לינקים','עבר טיוב - שם השירות','להעברה לתיקוף רווחה', 'cards']
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
    prepared_services_df = prepare_airtable_dataframe(df=services_airtable_df, key_field="id", airtable_key="id", fields_to_update=fields_to_copy)
    prepared_services_df = prepared_services_df.where(pd.notnull(prepared_services_df), None)
    if prepared_services_df.empty:
        return 0
    return update_airtable_records(
        df=prepared_services_df,
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        key_field="id",
        retrieve_not_updated_ids=True
    )


def get_matching_dataframes() ->Tuple[pd.DataFrame, pd.DataFrame]:

    branches_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_BRANCH_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE
    )
    services_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE
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
    fix_manual_data(branches_df=branches_df, services_df=services_df)
    fix_services_data(services_df=services_df)


if __name__ == '__main__':
    run(None, None, None)
