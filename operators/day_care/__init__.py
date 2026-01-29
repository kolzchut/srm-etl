from operators.day_care.fetch_as_df import fetch_as_df
from operators.day_care.match_organizations import match_organizations
from operators.day_care.update_branch import update_branch
from operators.day_care.update_organization import update_organization
from operators.day_care.update_service import update_service
from srm_tools.error_notifier import invoke_on
from conf import settings
from extract.extract_data_from_airtable import load_airtable_as_dataframe

def remove_unnecessary_records_dataframe(df):
    df = df[df["תיאור סוג מעון"].isin(["משפחתון", "צהרון"])]
    df.loc[:, "מספר טלפון"] = (
        df["מספר טלפון"]
        .astype(str)
        .str.replace(r"\D", "", regex=True)
    )
    df = df[df["מספר טלפון"].str.len().between(8, 11)]
    df = df.drop(columns=['תפוסת בוגרים', 'תפוסת פעוטות', 'תפוסת תינוקות'])
    return df

def fix_records(df):
    df["מספר טלפון"] = df["מספר טלפון"].astype(str).apply(
        lambda x: "0" + x if not x.startswith("0") else x
    )

    df["ח.פ. ארגון"] = df["ח.פ. ארגון"].astype(str).apply(
        lambda x: x[2:-2] if len(x) >= 13 and x.startswith("11") else x
    )
    return df

def replace_name(name):
    name = name.strip()
    if name.startswith(("מ. אזורית", "מ.א.", "מ.א")):
        return name.replace("מ. אזורית", "מועצה אזורית") \
                   .replace("מ.א.", "מועצה אזורית") \
                   .replace("מ.א", "מועצה אזורית")
    elif name.startswith(("מ. מקומית", "מ.מקומית", "מ.מ.")):
        return name.replace("מ. מקומית", "מועצה מקומית") \
                   .replace("מ.מקומית", "מועצה מקומית") \
                   .replace("מ.מ.", "מועצה מקומית")
    return name

def enrich_records(df):
    df['שם ארגון'] = df['שם ארגון'].apply(replace_name)
    df['source'] = 'mol_daycare'
    df['status'] = 'ACTIVE'
    return df


def run(*_):
    print(f'Fetching and transforming data...')
    df = fetch_as_df()
    df = remove_unnecessary_records_dataframe(df)
    df = fix_records(df)
    df = enrich_records(df)
    organizations_airtable_df = load_airtable_as_dataframe(
        table_name=settings.AIRTABLE_ORGANIZATION_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE
    )
    df = match_organizations(
        fetched_df=df,
        fetched_field='שם ארגון',
        airtable_df=organizations_airtable_df,
        airtable_field='x_final_org_name'
    )


    print(f'Updating Airtable...')

    modified_services=update_service()
    print(f"Effected {modified_services} services.")

    modified_organizations=update_organization(df=df.copy())
    print(f"Effected {modified_organizations} organizations.")

    modified_branches=update_branch(df=df.copy())
    print(f"Effected {modified_branches} branches.")


def operator(*_):
    invoke_on(run, 'day_care')

if __name__ == '__main__':
    run(None, None, None)
