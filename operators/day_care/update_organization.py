from conf import settings
from extract.extract_data_from_airtable import load_airtable_as_dataframe
from load.airtable import update_if_exists_if_not_create
from operators.meser_new.utilities.trigger_status_check import trigger_status_check
from utilities.update import prepare_airtable_dataframe


def filter_records_that_exists_in_organization_table(df):
    all_organizations_from_table = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_ORGANIZATION_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        view=settings.AIRTABLE_VIEW,
        api_key=settings.AIRTABLE_API_KEY
    )
    if all_organizations_from_table.empty:
        return df

    existing_ids = all_organizations_from_table['id'].tolist()

    return df[~df['id'].isin(existing_ids)]



def rename_dataframe(df):
    df = df.rename(columns={
        "ח.פ. ארגון": "id",
        "שם ארגון": "name",
    })
    return df

def update_organization(df):
    df = rename_dataframe(df)
    df['id'] = df['id'].astype(str)
    fields_to_update = ["id", "name", "source", "status"]
    trigger_status_check(df=df, table_name=settings.AIRTABLE_ORGANIZATION_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                         airtable_key_field='id', active_value='ACTIVE', inactive_value='INACTIVE',
                         only_from_source='mol_daycare', df_key_field='id', batch_size=50)
    # df =filter_records_that_exists_in_organization_table(df)
    prepare_df = prepare_airtable_dataframe(df=df, key_field="id",airtable_key="id",fields_to_update=fields_to_update)
    modified = update_if_exists_if_not_create(df=prepare_df, table_name=settings.AIRTABLE_ORGANIZATION_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE, airtable_key="id")
    return modified
