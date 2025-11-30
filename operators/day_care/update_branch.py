from conf import settings
from load.airtable import update_if_exists_if_not_create
from operators.meser_new.utilities.get_foreign_key_by_field import get_foreign_key_by_field
from operators.meser_new.utilities.trigger_status_check import trigger_status_check
from srm_tools.hash import hasher
from utilities.update import prepare_airtable_dataframe
import pandas as pd

def ensure_list_fields(df:pd.DataFrame, columns: list):
    """
    Ensures that the given columns contain real Python lists, not stringified lists.
    Converts values like "['recABC']" → ['recABC'] safely.
    """
    def str_to_list(s):
        if not s or s == '[]':
            return []
        s = s.strip("[]")
        items = [item.strip().strip("'\"") for item in s.split(",") if item.strip()]
        return items

    for col in columns:
        if col in df.columns:
            df[col] = df[col].apply(
                lambda x: str_to_list(x) if isinstance(x, str) else (x if isinstance(x, list) else [])
            )
    return df

def transform_dataframe_to_branch(df):
    df = df.rename(columns={
        "מספר טלפון": "phone_numbers",
        "שם מעון": "name",
    })
    df['id'] = df.apply(lambda row: 'mol_daycare-' + hasher(str(row['סמל מעון']) + str(row['ח.פ. ארגון'])),axis=1)
    df['source'] = 'mol_daycare'
    make_address = lambda x: f'{x.get("שם עיר", "")}, {x.get("שם רחוב", "")} {x.get("מספר בית", "")}'.strip(", ")
    df['location'] = df.apply(make_address, axis=1)
    df['address'] = df.apply(make_address, axis=1)
    df['description'] = df.apply(lambda x: f'{x.get("שם מנהל", "")} \n {x.get("סמל מעון", "")}')
    df['status'] = 'ACTIVE'

    df['service_id_matcher'] = df.apply(
        lambda x: "mol_daycare-1" if x['תיאור סוג מעון'] == 'משפחתון' else
        "mol_daycare-2" if x['תיאור סוג מעון'] == 'צהרון' else
        "mol_daycare-0",
        axis=1
    )

    return df

def load_foreign_keys(df):
    df['organization_id'] = df['ח.פ. ארגון']
    df = get_foreign_key_by_field(
        df=df,
        current_table=settings.AIRTABLE_BRANCH_TABLE,
        source_table=settings.AIRTABLE_ORGANIZATION_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        base_field="organization_id",
        target_field="organization",
        airtable_key="id"
    )
    df = get_foreign_key_by_field(
        df=df,
        current_table=settings.AIRTABLE_BRANCH_TABLE,
        source_table=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        base_field="service_id_matcher",
        target_field="services",
        airtable_key="id"
    )
    return df

def clean_fields(df, fields_to_update):
    for col in fields_to_update:
        if col in df.columns:
            df[col] = df[col].fillna('').astype(str)
    return df

def update_branch(df):
    df = transform_dataframe_to_branch(df)
    fields_to_update = ["source","location","address","phone_numbers","description","status","id", "organization", "services"]
    trigger_status_check(df=df, table_name=settings.AIRTABLE_BRANCH_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                         airtable_key_field='id', active_value='ACTIVE', inactive_value='INACTIVE',
                         only_from_source='mol_daycare', df_key_field='id', batch_size=50)
    df = load_foreign_keys(df)
    df = clean_fields(df, fields_to_update)
    df = df.where(pd.notnull(df), None)

    df = ensure_list_fields(df=df, columns=['services', 'organization'])

    prepare_df = prepare_airtable_dataframe(df=df, fields_to_update=fields_to_update, key_field='id', airtable_key='id')

    modified = update_if_exists_if_not_create(df=prepare_df,airtable_key='id',base_id=settings.AIRTABLE_DATA_IMPORT_BASE,table_name=settings.AIRTABLE_BRANCH_TABLE,batch_size=50)
    return modified
