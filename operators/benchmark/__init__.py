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

    try:
        results = DF.Flow(
            load_from_airtable(
                'appkZFe6v5H63jLuC',
                'Results',
                settings.AIRTABLE_VIEW,
                settings.AIRTABLE_API_KEY
            ),
        ).results()[0][0]
    except Exception as e:
        logger.error('Failed to load results: %s', e)
        return

    result_mapping = {}
    for x in results:
        result_mapping[x.get('id', 'N/A')] = dict(
            __key=x.get(AIRTABLE_ID_FIELD, 'N/A'),
            id=x.get('id', 'N/A'),
            Decision=x.get('Decision', 'N/A'),
            Query=x.get('Query', 'N/A'),
            Response=x.get('Response', 'N/A'),
            Name=x.get('Name', 'N/A'),
            Organization=x.get('Organization', 'N/A'),
            Situation=x.get('Situation', 'N/A')
        )

    logger.info('Loaded %d results', len(result_mapping))

    for x in result_mapping.values():
        logger.info(
            '__key=%s | id=%s | Decision=%s | Query=%s | Response=%s | Name=%s | Organization=%s | Situation=%s',
            str(x['__key']),
            str(x['id']),
            str(x['Decision']),
            str(x['Query']),
            str(x['Response']),
            str(x['Name']),
            str(x['Organization']),
            str(x['Situation'])
        )



def operator(*_):
    logger.info('Running benchmarks')
    invoke_on(run_benchmark, 'Benchmark')
    logger.info('Finished running benchmarks')


if __name__ == '__main__':
    run_benchmark()
