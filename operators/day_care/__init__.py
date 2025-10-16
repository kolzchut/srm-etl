from operators.day_care.fetch_as_df import fetch_as_df
from operators.day_care.update_branch import update_branch
from operators.day_care.update_organization import update_organization
from operators.day_care.update_service import update_service
from srm_tools.error_notifier import invoke_on

def remove_unnecessary_records_dataframe(df):
    df = df[df["תיאור סוג מעון"].isin(["משפחתון", "צהרון"])]
    df = df[df["מספר טלפון"].astype(str).str.len().between(8, 11)]
    return df

def fix_records(df):
    df["מספר טלפון"] = df["מספר טלפון"].astype(str).apply(lambda x: "0" + x if not x.startswith("0") else x)
    df["ח.פ. ארגון"] = df["ח.פ. ארגון"].astype(str)
    return df

def replace_name(name):
    name = name.strip()
    if name.startswith(("מ. אזורית", "מ.א.", "מ.א")):
        return name.replace("מ. אזורית", "מועצה אזורית") \
                   .replace("מ.א.", "מועצה אזורית") \
                   .replace("מ.א", "מועצה אזורית")
    elif name.startswith(("מ. מקומית", "מ.מקומית")):
        return name.replace("מ. מקומית", "מועצה מקומית") \
                   .replace("מ.מקומית", "מועצה מקומית")
    return name

def enrich_records(df):
    df['שם ארגון'] = df['שם ארגון'].apply(replace_name)
    df['source'] = 'mol_daycare'
    df['status'] = 'ACTIVE'
    return df


def run(*_):
    print("Hola")
    df = fetch_as_df()
    df = remove_unnecessary_records_dataframe(df)
    df = fix_records(df)
    df = enrich_records(df)
    print(df.head(5))


    modified_services=update_service()
    print(f"Updated {modified_services} services.")
    modified_organizations=update_organization(df=df.copy())
    print(f"Updated {modified_organizations} organizations.")
    modified_branches=update_branch(df=df.copy())
    print(f"Updated {modified_branches} branches.")



def operator(*_):
    invoke_on(run, 'Day-Care')

if __name__ == '__main__':
    run(None, None, None)