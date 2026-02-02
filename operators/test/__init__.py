from extract.extract_data_from_airtable import load_airtable_as_dataframe
from conf import settings
import pandas as pd


def replace_airtable_ids_in_cards(branches_airtable_df, services_airtable_df,cards_airtable_df, matching_branches_df, matching_services_df):
    branch_id_map = dict(zip(matching_branches_df['old_branch_id'], matching_branches_df['new_branch_id']))
    service_id_map = dict(zip(matching_services_df['old_service_id'], matching_services_df['new_service_id']))

    def replace_ids_in_list(id_list, id_map):
        if not isinstance(id_list, list):
            return id_list
        return [id_map.get(item, item) for item in id_list]

    cards_airtable_df['Branches'] = cards_airtable_df['Branches'].apply(lambda x: replace_ids_in_list(x, branch_id_map))
    cards_airtable_df['Services'] = cards_airtable_df['Services'].apply(lambda x: replace_ids_in_list(x, service_id_map))

    return cards_airtable_df


def get_data():
    cards_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_CARDS_TABLE,
        base_id=settings.AIRTABLE_STAGING_BASE
    )
    branches_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_BRANCH_TABLE,
        base_id=settings.AIRTABLE_STAGING_BASE
    )
    services_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_STAGING_BASE
    )

    matching_branches_df = pd.read_csv('clean_matched_branches.csv')
    matching_services_df = pd.read_csv('clean_matched_services.csv')

    return branches_airtable_df, services_airtable_df,cards_airtable_df, matching_branches_df, matching_services_df


if __name__ == "__main__":
    branches_airtable_df, services_airtable_df,cards_airtable_df, matching_branches_df, matching_services_df = get_data()
    updated_cards_df = replace_airtable_ids_in_cards(branches_airtable_df, services_airtable_df,cards_airtable_df, matching_branches_df, matching_services_df)
    print("Data loaded successfully.")
