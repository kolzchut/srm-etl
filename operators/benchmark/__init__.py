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
    resp = requests.get(f'{BASE}/api/idx/get/{query}?type=autocomplete', timeout=TIMEOUT)
    if resp.status_code == 200:
        return resp.json()
    else:
        return None


def search_dym_autocomplete(query):
    params = dict(
        size=1,
        q=query,
        match_operator='and',
        filter=json.dumps([{'visible': True, 'low': False}]),
    )
    ret = requests.get(f'{BASE}/api/idx/search/autocomplete', params, timeout=TIMEOUT).json()
    if ret and 'search_results' in ret and ret['search_results']:
        return ret['search_results'][0]['source']['query']


def search_dym(query):
    SHARD_SIZE = 50
    params = dict(
        size=1,
        offset=0,
        extra='did-you-mean',
        match_operator='and',
        match_type='cross_fields',
        minscore=50,
        q=query,
    )
    resp = requests.get(f'{BASE}/api/idx/search/cards', params, timeout=TIMEOUT).json()
    pa = resp.get('possible_autocomplete')
    if pa:
        best = pa[0]
        total = resp.get('search_counts', {}).get('_current', {}).get('total_overall') or 0
        if total < 10:
            return
        best_doc_factor = math.log(len(best.get('key')))
        for item in pa[1:]:
            item['doc_count'] *= math.log(len(item.get('key'))) / best_doc_factor
        pa = sorted(pa, key=lambda x: x['doc_count'], reverse=True)
        best = pa[0]
        best_doc_count = best.get('doc_count') or 0
        threshold = min(SHARD_SIZE, total) / 3
        if best_doc_count <= SHARD_SIZE and best_doc_count > threshold and 'key' in best:
            return best['key']


def make_filter(ac):
    ret = dict()
    if ac.get('response'):
        ret['response_ids_parents'] = ac['response']
    if ac.get('situation'):
        ret['situation_ids'] = ac['situation']
    if ac.get('org_id'):
        ret['organization_id'] = ac['org_id']
    return ret or None


def search_cards(query, ac):
    ret = []
    for n in (False, True, None):
        params = dict(
            size=20,
            offset=0,
        )
        ff = dict()
        if ac is None:
            params['q'] = query
            params['minscore'] = 50
            params['match_type'] = 'cross_fields'
            params['match_operator'] = 'and';
        else:
            params['q'] = ac['structured_query']
            params['match_operator'] = 'or';
            ff = make_filter(ac) or ff

        if n is not None:
            filters = dict(**ff, national_service=n)
        else:
            filters = ff
        if filters:
            params['filter'] = json.dumps([filters])

        params['extra'] = 'national-services|collapse|collapse-collect'

        resp = requests.get(f'{BASE}/api/idx/search/cards', params, timeout=TIMEOUT)
        assert resp.status_code == 200
        resp = resp.json()
        if 'search_results' in resp:
            ret.append(
                ([x['source'] for x in resp['search_results']], resp['search_counts']['_current']['total_overall']))
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
                Position=pos + 1
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
        search_results = search_cards(query, ac)
        row['Number of results'] = search_results[2][1]

        all_results = list(enumerate(search_results[0][0])) + list(enumerate(search_results[1][0]))
        total = 0
        score = 0
        for i, card in all_results:
            decisions = [
                add_to_found(i, query, card['service_id'], card['service_name'], 'Service', used)
                if not card['national_service'] else
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


def run_benchmark(*_):
    ## DEPRECATED OPERATOR
    logger.warn("Deprecated, No longer in use, based on  SRM SEARCH base.")
    return
    if not check_api_health():
        raise RuntimeError(f"API at {BASE} appears to be unavailable. Skipping benchmark run.")
    results = DF.Flow(
        load_from_airtable('appkZFe6v5H63jLuC', 'Results', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    ).results()[0][0]
    result_mapping = {x['id']: dict(__key=x[AIRTABLE_ID_FIELD], id=x['id'], Decision=x['Decision']) for x in results}
    print('Loaded', len(result_mapping), 'results')

    history = DF.Flow(
        load_from_airtable('appkZFe6v5H63jLuC', 'History', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.sort_rows('Date'),
        DF.join_with_self('History', ['Query'], {
            'Query': None,
            'Number of results': dict(aggregate='last'),
            'Upgrade Suggestion': dict(aggregate='last'),
            'Score': dict(aggregate='last'),
            'Date': dict(aggregate='last'),
        })
    ).results()[0][0]
    history = {x['Query']: x for x in history}

    found = []
    bad_performers = set()
    benchmarks = DF.Flow(
        load_from_airtable('appkZFe6v5H63jLuC', 'Benchmark', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.filter_rows(lambda r: r['Query'] != 'dummy'),
        run_single_benchmark(found, result_mapping, bad_performers),
        dump_to_airtable({
            ('appkZFe6v5H63jLuC', 'Benchmark'): {
                'resource-name': 'Benchmark',
                'typecast': True
            }
        }, settings.AIRTABLE_API_KEY),
    ).results()[0][0]
    new_history = []
    for b in benchmarks:
        if b['Query'] in history:
            if b['Score'] == history[b['Query']]['Score'] and b['Upgrade Suggestion'] == history[b['Query']][
                'Upgrade Suggestion']:
                continue
            else:
                print('Changed record for', b['Query'], b['Score'], '!=', history[b['Query']]['Score'],
                      repr(b['Upgrade Suggestion']), '!=', repr(history[b['Query']]['Upgrade Suggestion']))
            if not b['Upgrade Suggestion'] and not history[b['Query']]['Upgrade Suggestion']:
                continue
            if b['Score'] and history[b['Query']]['Score']:
                if (b['Score'] - history[b['Query']]['Score']) < 0.1:
                    continue
        if not b['Score']:
            continue
        new_history.append({
            'Query': b['Query'],
            'Score': b['Score'],
            'Upgrade Suggestion': b['Upgrade Suggestion'],
            'Number of results': b['Number of results'],
            'Date': datetime.datetime.now().isoformat()
        })
    DF.Flow(
        new_history,
        DF.update_resource(-1, name='History'),
        dump_to_airtable({
            ('appkZFe6v5H63jLuC', 'History'): {
                'resource-name': 'History',
                'typecast': True
            }
        }, settings.AIRTABLE_API_KEY),
    ).process()

    for f in found:
        f[AIRTABLE_ID_FIELD] = result_mapping.get(f['id'], dict()).get('__key')
        f['Bad Performer'] = f['id'] in bad_performers
    found_ids = set(x['id'] for x in found)
    for f in results:
        if f['id'] in found_ids:
            continue
        if f['id'] == 'dummy':
            continue
        f['Found'] = False
        f['Bad Performer'] = False
        f['Position'] = None
        for t in ('Service', 'Organization', 'Response', 'Situation', 'Benchmark'):
            if f.get(t) and isinstance(f[t], list) and len(f[t]) == 1:
                f[t] = f[t][0]
            else:
                f[t] = None
        found.append(f)

    DF.Flow(
        found,
        DF.update_resource(-1, name='found'),
        dump_to_airtable({
            ('appkZFe6v5H63jLuC', 'Results'): {
                'resource-name': 'found',
                'typecast': True
            }
        }, settings.AIRTABLE_API_KEY),
        DF.printer(),
    ).process()


def check_api_health(timeout=5):
    try:
        resp = requests.get(f'{BASE}/api/db/query?query=select%201',
                            timeout=timeout)  # just a simple query to check if the API is up
        return resp.status_code == 200
    except requests.exceptions.RequestException:
        return False


def operator(*_):
    logger.info('Running benchmarks')
    invoke_on(run_benchmark, 'Benchmark')
    logger.info('Finished running benchmarks')


if __name__ == '__main__':
    run_benchmark()
