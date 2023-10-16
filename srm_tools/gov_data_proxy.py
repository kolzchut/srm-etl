import requests
import time

from conf import settings

from srm_tools.scraping_utils import overcome_blocking


session = requests.Session()


def gov_data_proxy(template_id, skip):
    data = {
        'DynamicTemplateID': template_id,
        'QueryFilters': {'skip': {'Query': skip}},
        'From': skip,
    }
    timeout = 30
    resp = overcome_blocking(
        session,
        lambda: session.post(
            settings.GOV_DATA_PROXY,
            json=data,
            timeout=timeout,
        )
    )
    try:
        response = resp.json()
    except:
        print(resp.content)
        raise
    total, results = response['TotalResults'], response['Results']

    return total, results


def collect_gov_rows(template_id):
    skip = 0
    # seems to only ever return 10 results in a call
    skip_by = 10
    total, results = gov_data_proxy(template_id, skip)
    yield from results

    while len(results) < total:
        skip += skip_by
        for _ in range(3):
            _, batch = gov_data_proxy(template_id, skip)
            if len(batch) > 0:
                yield from batch
                print(f'SKIPPED {skip}, TOTAL {total}')
                total += len(batch)
                break
            time.sleep(10)
        else:
            break
    print(f'FETCHED {total} FOR {template_id}')
    assert total > 0
