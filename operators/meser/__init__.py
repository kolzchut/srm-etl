from operators.meser.update_service import update_airtable_services_from_df
from srm_tools.hash import hasher
from openlocationcode import openlocationcode as olc
from extract.extract_data_from_airtable import load_airtable_as_dataframe
from operators.meser.extract_meser_data import datagovil_fetch_and_transform_to_dataframe
from operators.meser.update_organization import update_airtable_organizations_from_df
from operators.meser.update_branch import update_airtable_branches_from_df
from srm_tools.error_notifier import invoke_on
import pandas as pd
from conf import settings
from srm_tools.logger import logger


DATA_SOURCE_ID = 'meser'

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


def create_address_clean(adrees, city_name):
    def clean(val):
        s = str(val).strip()
        return s if pd.notna(val) and s.lower() not in ['none', 'nan', ''] else None

    addr, city = clean(adrees), clean(city_name)

    if addr and city and addr.lower() == city.lower():
        addr = None

    return ' '.join(filter(None, [addr, city]))



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

    # Clean 'Adrees' and handle matching with 'City_Name'
    df['Adrees'] = df['Adrees'].astype(str).str.replace('999', '', regex=False).str.strip()
    df.loc[df['Adrees'] == df['City_Name'], 'Adrees'] = None

    # Create full 'address' field, avoiding duplicates when adrees equals city_name
    df['address'] = df.apply(
        lambda r: create_address_clean(r['Adrees'], r['City_Name']),
        axis=1
    ).str.strip()

    # phone_numbers
    df['phone_numbers'] = df['Telephone'].apply(
        lambda x: ""
        if pd.isna(x) or str(x).strip() in ['', '0']
        else ('0' + str(x) if str(x)[0] != '0' else str(x))
    )
    # tagging array
    df['tagging'] = df[['Type_Descr', 'Target_Population_Descr', 'Second_Classific', 'Gender_Descr', 'Head_Department']].apply(
        lambda row: [v for v in row if v not in [None, 'None', '']], axis=1
    )
    df['branch_id'] = df.apply(
        lambda r: 'meser-b-' + r['meser_id'],
        axis=1
    )
    df['service_id'] = df.apply(
        lambda r: 'meser-s-' + r['meser_id'],
        axis=1
    )


    # 4. Combine duplicates (same service_name + phone + address + organization_id + Owner_Code_Descr)
    grouped = df.groupby(['service_name', 'phone_numbers', 'address', 'organization_id'], dropna=False).agg({
        'service_id': 'first',
        'branch_id': 'first',
        'branch_name': 'first',
        'meser_id': 'first',
        'Owner_Code_Descr': 'first',
        'City_Name': 'first',
        'tagging': lambda x: flatten_and_deduplicate_list_of_lists(x)
    }).reset_index()


    # 5. pluscode from first available GisX/GisY in the group
    grouped['pluscode'] = df.groupby(['service_name', 'phone_numbers', 'address', 'organization_id'])[['GisY','GisX']].first().apply(
        lambda r: olc.encode(r['GisY'], r['GisX']) if pd.notna(r['GisY']) and pd.notna(r['GisX']) else None,
        axis=1
    ).values

    # 6. responses and situations from tags
    grouped['responses'] = grouped['tagging'].apply(
        lambda tags_list: flatten_and_deduplicate_list_of_lists(
            safe_list(tags.get(t.strip(), {}).get('response_ids')) for t in tags_list
        )
    )

    grouped['situations'] = grouped['tagging'].apply(
        lambda tags_list: flatten_and_deduplicate_list_of_lists(
            safe_list(tags.get(t.strip(), {}).get('situation_ids')) for t in tags_list
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
    logger.info("Starting Meser data update...")


    logger.info("Fetching and sanitizing source data...")
    # 1. Fetch source data from DataGovIL and sanitize
    df = datagovil_fetch_and_transform_to_dataframe()
    df = sanitize_for_airtable(df)

    logger.info("Loading tagging data...")
    # 2. Load tagging data from Airtable as DataFrame
    tags_df = load_airtable_as_dataframe(
        table_name='meser-tagging',
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        view=settings.AIRTABLE_VIEW,
        api_key=settings.AIRTABLE_API_KEY
    )

    logger.info("Processing tagging data...")
    # 3. Filter out invalid tags
    tags_df = tags_df[~tags_df['tag'].isin([None, 'dummy'])]
    tags_df = tags_df[['tag', 'response_ids', 'situation_ids']]

    # 4. Convert to dict for fast lookup
    tags = {row['tag']: {'response_ids': row['response_ids'], 'situation_ids': row['situation_ids']}
            for _, row in tags_df.iterrows()}

    logger.info("Transforming Meser data...")
    # 5. Transform the sanitized dataframe
    transformed_df = transform_meser_dataframe(df, tags)
    ### COMMENTED TO DISABLE LOCAL AUTHORITY SPECIAL HANDLING - DIFFERENT ISSUE - ACTUALLY SHOULD BE IN PRODUCTION ###
    # transformed_df_local, transformed_df_non_local = split_by_local_authority(transformed_df)
    #
    # logger.info("Handling local authorities...")
    # # 6. Handle local authorities separately
    # transformed_df_local = handle_local_authorities(transformed_df_local)
    # transformed_df = pd.concat([transformed_df_local, transformed_df_non_local], ignore_index=True)

    ## Remove that Line after fixing local authorities issue ##
    transformed_df = transformed_df[transformed_df["organization_id"].str.len().between(5, 15)]
    ### END COMMENTED TO DISABLE LOCAL AUTHORITY SPECIAL HANDLING - DIFFERENT ISSUE - ACTUALLY SHOULD BE IN PRODUCTION ###

    logger.info("Updating Airtable")
    modified_organizations = update_airtable_organizations_from_df(transformed_df.copy())
    logger.info(f"Effected {modified_organizations} organizations")

    modified_branches = update_airtable_branches_from_df(transformed_df.copy())
    logger.info(f"Effected {modified_branches} branches")

    modified_services = update_airtable_services_from_df(transformed_df.copy())
    logger.info(f"Effected {modified_services} services")

    logger.info("End Meser data update...")



def operator(*_):
    invoke_on(run, 'meser')


if __name__ == '__main__':
    run(None, None, None)
