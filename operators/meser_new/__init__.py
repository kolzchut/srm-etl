from operators.meser_new.local_authorities import handle_local_authorities
from operators.meser_new.update_service import update_airtable_services_from_df
from srm_tools.hash import hasher
from openlocationcode import openlocationcode as olc
from extract.extract_data_from_airtable import load_airtable_as_dataframe
from operators.meser_new.extract_meser_data import datagovil_fetch_and_transform_to_dataframe
from operators.meser_new.update_organization import update_airtable_organizations_from_df
from operators.meser_new.update_branch import update_airtable_branches_from_df
from srm_tools.error_notifier import invoke_on
import pandas as pd
from conf import settings

import hashlib


# Helper functions

def flatten_and_deduplicate_list_of_lists(lst_of_lsts):
    """Flatten nested lists and deduplicate preserving order."""
    seen = set()
    result = []
    for lst in lst_of_lsts:
        if lst is None:
            continue
        if not isinstance(lst, list):
            lst = [lst]
        for item in lst:
            if item is None or item == 'None':
                continue
            if item not in seen:
                seen.add(item)
                result.append(item)
    return result

def hasher(*args):
    s = "_".join([str(a) for a in args if a is not None])
    return hashlib.md5(s.encode()).hexdigest()[:8]


def flatten_and_deduplicate(lst):
    seen = set()
    result = []
    for item in lst:
        if item not in seen:
            result.append(item)
            seen.add(item)
    return result

def safe_list(lst):
    if isinstance(lst, list):
        return lst
    return []



def transform_meser_dataframe(df: pd.DataFrame, tags: dict) -> pd.DataFrame:
    """
    Transform DataFrame like the original DF.Flow:
    - Derived fields
    - branch_id (hash of address + organization_id)
    - service_id (hash of service_name + phone + address + org_id + branch_id)
    - Combine duplicates by service_name + phone + address + organization_id
    - Flatten tagging
    - Add pluscode, responses, situations
    """
    # 1. Derived fields
    df['service_name'] = df['Name'].str.strip()
    df['branch_name'] = df['Type_Descr'].str.strip()
    df = df.rename(columns={'Misgeret_Id': 'meser_id'})


    # organization_id
    df['organization_id'] = df['ORGANIZATIONS_BUSINES_NUM'].combine_first(df['Registered_Business_Id'])
    df['organization_id'] = df['organization_id'].fillna('500106406')  # like original

    # Clean Adrees
    df['Adrees'] = df['Adrees'].replace('999', '').str.strip()
    df['Adrees'] = df.apply(lambda r: None if r['Adrees'] == r['City_Name'] else r['Adrees'], axis=1)

    # Full address
    df['address'] = (df['Adrees'].fillna('') + ' ' + df['City_Name'].fillna('')).str.replace(' - ', '-').str.strip()

    # phone_numbers
    df['phone_numbers'] = df['Telephone'].apply(
        lambda x: '0' + str(x) if pd.notna(x) and str(x) != '' and str(x)[0] != '0'
        else (str(x) if pd.notna(x) else None)
    )

    # tagging array
    df['tagging'] = df[['Type_Descr', 'Target_Population_Descr', 'Second_Classific', 'Gender_Descr', 'Head_Department']].apply(
        lambda row: [v for v in row if v not in [None, 'None', '']], axis=1
    )

    # 2. branch_id = hash(address + organization_id)
    df['branch_id'] = df.apply(lambda r: 'meser-' + hasher(r['address'], r['organization_id']), axis=1)

    # 3. service_id = hash(service_name + phone + address + org_id + branch_id)
    df['service_id'] = df.apply(
        lambda r: 'meser-' + hasher(r['service_name'], r['phone_numbers'], r['address'], r['organization_id'], r['branch_id']),
        axis=1
    )

    # 4. Combine duplicates (same service_name + phone + address + organization_id + Owner_Code_Descr)
    grouped = df.groupby(['service_name', 'phone_numbers', 'address', 'organization_id' ], dropna=False).agg({
        'branch_id': 'first',
        'branch_name': 'first',
        'meser_id': 'first',
        'Owner_Code_Descr': 'first',
        'City_Name': 'first',
        'tagging': lambda x: flatten_and_deduplicate_list_of_lists(x)
    }).reset_index()

    # recompute service_id after grouping to match original hash logic
    grouped['service_id'] = grouped.apply(
        lambda r: 'meser-' + hasher(r['service_name'], r['phone_numbers'], r['address'], r['organization_id'], r['branch_id']),
        axis=1
    )

    # 5. pluscode from first available GisX/GisY in the group
    grouped['pluscode'] = df.groupby(['service_name', 'phone_numbers', 'address', 'organization_id'])[['GisY','GisX']].first().apply(
        lambda r: olc.encode(r['GisY'], r['GisX']) if pd.notna(r['GisY']) and pd.notna(r['GisX']) else None,
        axis=1
    ).values

    # 6. responses and situations from tags
    grouped['responses'] = grouped['tagging'].apply(
        lambda tags_list: flatten_and_deduplicate_list_of_lists(
            safe_list(tags.get(t, {}).get('response_ids')) for t in tags_list
        )
    )

    grouped['situations'] = grouped['tagging'].apply(
        lambda tags_list: flatten_and_deduplicate_list_of_lists(
            safe_list(tags.get(t, {}).get('situation_ids')) for t in tags_list
        )
    )

    return grouped




def sanitize_for_airtable(df: pd.DataFrame) -> pd.DataFrame:
    MISSING_VALUES = ['NULL', '-1', 'לא ידוע', 'לא משויך', 'רב תכליתי']
    numeric_fields = ["Actual_Capacity", "From_Age", "To_Age", "GisX", "GisY"]
    date_fields = ["STARTD"]  # add all other date fields here

    for col in df.columns:
        if col in numeric_fields:
            # Convert numeric fields to int, fill NaN with 0
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
        elif col in date_fields:
            # Convert dates to ISO format YYYY-MM-DD
            df[col] = pd.to_datetime(df[col], dayfirst=True, errors='coerce')
            df[col] = df[col].dt.strftime('%Y-%m-%d')
            df[col] = df[col].where(df[col].notna(), None)  # convert NaT to None
        else:
            # Convert all other fields to string and replace "magic values" with None
            df[col] = df[col].astype(str)
            df[col] = df[col].replace(MISSING_VALUES, None)

    return df

def split_by_local_authority(df):
    mask = df['Owner_Code_Descr'].str.contains(r'רשות מקומית', regex=True, na=False)
    return df[mask], df[~mask]

def run(*_):
    print("Starting Meser data update...")


    print("Fetching and sanitizing source data...")
    # 1. Fetch source data from DataGovIL and sanitize
    df = datagovil_fetch_and_transform_to_dataframe()
    df = sanitize_for_airtable(df)

    print("Loading tagging data...")
    # 2. Load tagging data from Airtable as DataFrame
    tags_df = load_airtable_as_dataframe(
        table_name='meser-tagging',
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        view=settings.AIRTABLE_VIEW,
        api_key=settings.AIRTABLE_API_KEY
    )

    print("Processing tagging data...")
    # 3. Filter out invalid tags
    tags_df = tags_df[~tags_df['tag'].isin([None, 'dummy'])]
    tags_df = tags_df[['tag', 'response_ids', 'situation_ids']]

    # 4. Convert to dict for fast lookup
    tags = {row['tag']: {'response_ids': row['response_ids'], 'situation_ids': row['situation_ids']}
            for _, row in tags_df.iterrows()}

    print("Transforming Meser data...")
    # 5. Transform the sanitized dataframe
    transformed_df = transform_meser_dataframe(df, tags)
    transformed_df_local, transformed_df_non_local = split_by_local_authority(transformed_df)

    print("Handling local authorities...")
    # 6. Handle local authorities separately
    transformed_df_local = handle_local_authorities(transformed_df_local)
    transformed_df = pd.concat([transformed_df_local, transformed_df_non_local], ignore_index=True)

    print("Updating Airtable")
    modified_organizations = update_airtable_organizations_from_df(transformed_df.copy())
    print(f"Modified {modified_organizations} organizations")

    modified_branches = update_airtable_branches_from_df(transformed_df.copy())
    print(f"Modified {modified_branches} branches")

    modified_services = update_airtable_services_from_df(transformed_df.copy())
    print(f"Modified {modified_services} services")

    print("End Meser data update...")



def operator(*_):
    invoke_on(run, 'Meser')


if __name__ == '__main__':
    run(None, None, None)
