from extract.extract_data_from_api import fetch_json_from_api
from transform.json_to_dataframe import json_to_dataframe
from conf import settings


def fetch_as_df():
    df = json_to_dataframe(fetch_json_from_api(api_url=settings.DAYCARE_API_URL).get('result')) ## Be aware of the resource ID
    return df
