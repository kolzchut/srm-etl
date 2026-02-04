from conf import settings
from extract.extract_data_from_airtable import load_airtable_as_dataframe
from load.airtable import update_if_exists_if_not_create
from operators.meser.utilities.trigger_status_check import trigger_status_check
from utilities.update import prepare_airtable_dataframe


def setup_kind(df):
    if 'שם ארגון' in df.columns:
        council_mask = (df['שם ארגון'].astype(str).str.contains("מועצה מקומית|מועצה אזורית", na=False))
        df['kind'] = 'רשות מקומית'
        df.loc[~council_mask, 'kind'] = 'חברה פרטית'
    else:
        df['kind'] = 'חברה פרטית'
    return df

def rename_dataframe(df):
    df = df.rename(columns={
        "ח.פ. ארגון": "id",
        "שם ארגון": "name",
    })
    return df

def update_organization(df):
    df = rename_dataframe(df)
    df = setup_kind(df)
    df['id'] = df['id'].astype(str)
    df = df[df['id'].str.len() >= 5]
    fields_to_prepare = ["id", "name", "source", "status", "kind"]
    trigger_status_check(df=df, table_name=settings.AIRTABLE_ORGANIZATION_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                         airtable_key_field='id', active_value='ACTIVE', inactive_value='INACTIVE',
                         only_from_source='mol_daycare', df_key_field='id', batch_size=50)
    prepare_df = prepare_airtable_dataframe(df=df, key_field="id",airtable_key="id",fields_to_prepare=fields_to_prepare)
    modified = update_if_exists_if_not_create(df=prepare_df, table_name=settings.AIRTABLE_ORGANIZATION_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE, airtable_key="id", fields_to_update= ["id", "source", "status"])
    return modified
