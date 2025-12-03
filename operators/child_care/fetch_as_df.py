import pandas as pd
import requests
from conf import settings
from datetime import datetime


def replace_current_year_macro(text):
    current_year = datetime.now().year
    return text.replace("%%CURRENT_YEAR%%", str(current_year + 1))


def fetch():
    url = replace_current_year_macro(settings.CHILDCARE_API_URL)
    response = requests.get(url)
    response.raise_for_status()
    return response.content


def transform(content):
    html_content = content.decode('utf-8')
    # Prefer the lxml flavor which is already installed, to avoid html5lib/bs4 optional deps
    tables = pd.read_html(html_content, flavor="lxml")
    df = tables[0]
    df.columns = df.iloc[0]
    df = df[1:]
    df.reset_index(drop=True, inplace=True)
    return df


def fetch_as_df():
    return transform(fetch())
