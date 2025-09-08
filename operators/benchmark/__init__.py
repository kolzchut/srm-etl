import json
from collections import Counter
import datetime
import math

import requests

import dataflows as DF

from srm_tools.logger import logger
from srm_tools.processors import fetch_mapper, update_mapper
from srm_tools.error_notifier import invoke_on
from dataflows_airtable import load_from_airtable, AIRTABLE_ID_FIELD, dump_to_airtable

from conf import settings

BASE = 'https://srm-staging-api.whiletrue.industries'
TIMEOUT = 20


def get_autocomplete(query):
    query = query.replace(' ', '_')
    resp = requests.get(f'{BASE}/autocomplete/{query}', timeout=TIMEOUT)
    if resp.status_code == 200:
        return resp.json()
    else:
        return None


def check_api_health(timeout=5):
    try:
        resp = requests.get(f'{BASE}/test',
                            timeout=timeout)  # just a simple query to check if the API is up
        return resp.status_code == 200
    except requests.exceptions.RequestException:
        return False


def run_benchmark():
    if not check_api_health():
        raise RuntimeError('API is not reachable')
    logger.info('API is reachable, starting benchmarks')
    results = DF.Flow(
        load_from_airtable('appkZFe6v5H63jLuC', 'Results', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    ).results()[0][0]
    result_mapping = {x['id']: dict(__key=x[AIRTABLE_ID_FIELD], id=x['id'], Decision=x['Decision']) for x in results}
    logger.info('Loaded', len(result_mapping), 'results')
    logger.info(result_mapping)



def operator(*_):
    logger.info('Running benchmarks')
    invoke_on(run_benchmark, 'Benchmark')
    logger.info('Finished running benchmarks')


if __name__ == '__main__':
    run_benchmark()
