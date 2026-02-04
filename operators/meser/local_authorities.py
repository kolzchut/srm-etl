import csv
import pandas as pd

from conf import settings
from load.airtable import update_if_exists_if_not_create
from utilities.update import prepare_airtable_dataframe
from srm_tools.logger import logger

def clean_city_name(city_series: pd.Series) -> pd.Series:
    city_series = city_series.str.replace(r'[-"\'`]', '', regex=True)
    city_series = city_series.str.replace(r'\s+', ' ', regex=True)
    city_series = city_series.str.strip()
    return city_series

def set_up_organizations(df: pd.DataFrame):
    df = df.drop(columns=['counsil_short_name'])
    airtable_key = "id"
    df = prepare_airtable_dataframe(df=df, key_field=airtable_key, airtable_key=airtable_key, fields_to_prepare=["id", "name", "status", "kind", "urls","phone_numbers"])
    update_if_exists_if_not_create(df=df, table_name=settings.AIRTABLE_ORGANIZATION_TABLE,base_id=settings.AIRTABLE_DATA_IMPORT_BASE, airtable_key=airtable_key)

def set_up_local_authorities():
    local_authorities_csv = csv.DictReader(open('static_data/local_authorities.csv', encoding='utf-8'))
    df = pd.DataFrame(local_authorities_csv)
    set_up_organizations(df)

def set_up_map_cities_with_local_authorities() -> pd.DataFrame:
    map_cities_with_local_authorities_csv = csv.DictReader(open('static_data/map_cities_with_local_authorities.csv', encoding='utf-8'))
    df = pd.DataFrame(map_cities_with_local_authorities_csv)
    return df


def handle_local_authorities(df_from_meser: pd.DataFrame) -> pd.DataFrame:
    try:
        set_up_local_authorities()
        df_from_map_cities_with_local_authorities = set_up_map_cities_with_local_authorities()

        df_from_meser['City_Name'] = clean_city_name(df_from_meser['City_Name'])
        df_from_map_cities_with_local_authorities['city'] = clean_city_name(df_from_map_cities_with_local_authorities['city'])

        df_from_meser = df_from_meser.merge(
            df_from_map_cities_with_local_authorities[['city', 'counsil_id']],
            left_on='City_Name',
            right_on='city',
            how='left'
        )

        df_from_meser['organization_id'] = df_from_meser['counsil_id']
        df_from_meser.drop(columns=['counsil_id'], inplace=True)

        ## Drop rows where organization_id is NaN
        df_from_meser = df_from_meser.dropna(subset=['organization_id'])

        ## Safety check
        if not isinstance(df_from_meser, pd.DataFrame):
            return pd.DataFrame()
        return df_from_meser


    except Exception as e:
        logger.error(f"Error setting up local authorities: {e}")
        raise e
