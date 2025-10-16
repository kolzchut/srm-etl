import pandas as pd
import requests
from conf import settings
from datetime import datetime


def replace_current_year_macro(text):
    current_year = datetime.now().year
    return text.replace("%%CURRENT_YEAR%%", str(current_year)+1)

def fetch():
    url = settings.CHILDCARE_API_URL
    response = requests.get(url)
    return response.content

def transform(content):
    html_content = content.decode('utf-8')
    tables = pd.read_html(html_content)
    df = tables[0]
    df.columns = df.iloc[0]
    df = df[1:]
    df.reset_index(drop=True, inplace=True)
    return df

def fetch_as_df():
    return transform(fetch())
