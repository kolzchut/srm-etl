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
        resp = requests.get(f'{BASE}/test', timeout=timeout)
        return resp.status_code == 200
    except requests.exceptions.RequestException:
        return False


def make_filter(ac):
    ret = dict()
    if ac.get('response'):
        ret['response_ids_parents'] = ac['response']
    if ac.get('situation'):
        ret['situation_ids'] = ac['situation']
    if ac.get('org_id'):
        ret['organization_id'] = ac['org_id']
    return ret or None


def search_cards(searchQuery: str, responseId: str = "", situationId: str = "", by: str = ""):
    ret = []
    for n in (False, True, None):
        params = {
            "q": searchQuery,
            "minscore": 50,
            "match_type": "cross_fields",
            "match_operator": "and",
            "extra": "national-services|collapse|collapse-collect",
        }

        filters = {}
        if responseId:
            filters["response_ids_parents"] = responseId
        if situationId:
            filters["situation_ids"] = situationId
        if by:
            filters["organization_name"] = by
        if n is not None:
            filters["national_service"] = n

        if filters:
            params["filter"] = json.dumps([filters])

        resp = requests.get(f"{BASE}/api/idx/search/cards", params=params, timeout=TIMEOUT)
        resp.raise_for_status()
        resp_json = resp.json()

        if "search_results" in resp_json:
            ret.append((
                [x["source"] for x in resp_json["search_results"]],
                resp_json.get("search_counts", {}).get("_current", {}).get("total_overall", 0)
            ))
        else:
            ret.append(([], 0))

    return ret


def run_single_benchmark(found, result_mapping, bad_performers):

    def add_to_found(pos, query, id, name, type, used):
        found_id = f'{query}:{id}'
        if found_id not in used:
            item = dict(
                Service=None,
                Organization=None,
                Response=None,
                Situation=None,
            )
            item.update(dict(
                id=found_id,
                Benchmark=query,
                Name=name,
                Found=True,
                Type=type,
                Position=pos+1
            ))
            field = type.split(' ')[-1]
            item[field] = id
            found.append(item)
            used.add(found_id)
        if found_id in result_mapping:
            return result_mapping[found_id]['Decision']

    def func(row):
        used = set()
        query = row['Query'].strip()
        print('Query', query)
        row['Query'] = query
        ac = get_autocomplete(query)
        structured = ac is not None
        row['Structured'] = structured
        if structured:
            row['Upgrade Suggestion'] = None
        else:
            row['Upgrade Suggestion'] = search_dym(query) or search_dym_autocomplete(query)
        search_results = search_cards(query)  # ac not passed, original logic preserved
        row['Number of results'] = search_results[2][1]

        all_results = list(enumerate(search_results[0][0])) + list(enumerate(search_results[1][0]))
        total = 0
        score = 0
        for i, card in all_results:
            decisions = [
                add_to_found(i, query, card['service_id'], card['service_name'], 'Service', used)
                if not card.get('national_service') else
                add_to_found(i, query, card['service_id'], card['service_name'], 'National Service', used),
                add_to_found(i, query, card['organization_id'], card['organization_name'], 'Organization', used),
                *[add_to_found(i, query, response['id'], response['name'], 'Response', used) for response in
                  card.get('responses', [])],
                *[add_to_found(i, query, situation['id'], situation['name'], 'Situation', used) for situation in
                  card.get('situations', [])],
            ]
            decisions = list(filter(lambda x: x not in (None, 'Neutral'), decisions))
            ind_score = 0.89 ** i
            total += ind_score
            if decisions:
                decisions = Counter(decisions)
                top = decisions.most_common(1)[0][0]
                if top == 'Good':
                    pass
                elif top == 'Bad':
                    ind_score = -ind_score
                else:
                    assert False
            else:
                ind_score = 0
            score += ind_score
            if ind_score <= 0:
                bpid = f'{query}:{card["service_id"]}'
                if bpid not in bad_performers:
                    print('BAD PERFORMER', query, card['service_name'],
                          Counter(decisions).most_common() if decisions else None)
                    bad_performers.add(bpid)

        row['Score'] = 100 * score / total if total else None

    return func


def run_benchmark():
    if not check_api_health():
        raise RuntimeError('API is not healthy, aborting benchmarks')
    logger.info('API is healthy, starting benchmarks')

    results = DF.Flow(
        load_from_airtable('appkZFe6v5H63jLuC', 'Results', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    ).results()[0][0]
    result_mapping = {x['id']: dict(__key=x[AIRTABLE_ID_FIELD], id=x['id'], Decision=x['Decision']) for x in results}

    found = []
    bad_performers = set()

    logger.info(result_mapping, results)

    # benchmarks = DF.Flow(
    #     load_from_airtable('appkZFe6v5H63jLuC', 'Benchmark', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    #     DF.filter_rows(lambda r: r['Query'] != 'dummy'),
    #     run_single_benchmark(found, result_mapping, bad_performers),
    #     dump_to_airtable({
    #         ('appkZFe6v5H63jLuC', 'Benchmark'): {
    #             'resource-name': 'Benchmark',
    #             'typecast': True
    #         }
    #     }, settings.AIRTABLE_API_KEY),
    # ).results()[0][0]

    logger.info('Benchmark run completed')


def operator(*_):
    logger.info('Running benchmarks')
    invoke_on(run_benchmark, 'Benchmark')
    logger.info('Finished running benchmarks')


if __name__ == '__main__':
    run_benchmark()
