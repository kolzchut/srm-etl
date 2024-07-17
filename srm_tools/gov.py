import time

import requests

from srm_tools.logger import logger

HEADERS = {
    "User-Agent": "kz-data-reader",
    # 'User-Agent': 'curl/8.4.0',
    'Accept': '*/*'
}

def get_gov_api(url, skip):
    # Note: The gov API is buggy or just weird. It looks like you can set a high limit of items,
    # but the most that get returned in any payload is 50.
    # If you pass, for example, limit 1000, the stats on the response object will say:
    # {... "total":90,"start_index":0,"page_size":1000,"current_page":1,"pages":1...}
    # but this is wrong - you only have 50 items, and it turns out you need to iterate by using
    # skip. And then, the interaction between limit and skip is weird to me.
    # you need to set a limit higher than the results we will retreive, but whatever you put in limit
    # is used, minus start_index, to declare page_size, which is wrong ......
    # we are going to batch in 50s which seems to be the upper limit for a result set.
    # also, we get blocked sometimes, but it is not consistent - the retry logic is for that.
    timeout = 5
    wait_when_blocked = 180
    headers = HEADERS
    retries = 5
    while retries:
        try:
            # when we get blocked, we still get a success status code
            # we only know we have a bad payload by parsing it.
            # so we catch the exception on parsing as json.
            response = requests.get(
                url,
                params={'limit': skip+10, 'skip': skip},
                timeout=timeout,
                headers=headers,
            )
            response.raise_for_status()
            # print(f'GOT {response.status_code}: {response.content[:1000]}')
            # print(f'GOT2 {response.headers}')
            response = response.json()
            total, results = response['total'], response['results']
            retries = 0
        except Exception as e:
            total, results = 0, tuple()
            retries = retries - 1
            print(f'Gov API access blocked. Retrying in 3 minutes. {retries} retries left. ({e})')
            time.sleep(wait_when_blocked)

    if total == 0:
        msg = 'Gov API access blocked. Cannot complete data extraction.'
        logger.info(msg)
        raise Exception(msg)
    return total, results
