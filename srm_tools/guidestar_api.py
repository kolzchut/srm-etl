import os
import json
import time
import requests
from requests.api import head

from conf import settings

from srm_tools.logger import logger
from srm_tools.debug_cache import cache_get, cache_set

class GuidestarAPI():

    BASE = settings.GUIDESTAR_API
    TIMEOUT = 30
    _headers = None

    def __init__(self):
        self.branch_cache = dict()

    def to_json(self, callable):
        resp = None
        for _ in range(10):
            try:
                resp = callable()
                return resp
            except Exception as e:
                try:
                    logger.error(resp.text[:100])
                except:
                    pass
                logger.error('FAILED TO FETCH FROM GUIDESTAR API, retrying in 30 seconds... {}'.format(str(e)))
                time.sleep(30)
        logger.error('Failed to get response from Guidestar API')
        
    def login(self, username, password):
        resp = self.to_json(lambda: requests.post(f'{self.BASE}/login', json=dict(username=username, password=password)).json())
        sessionId = resp['sessionId']
        headers = dict(
            Authorization=f'Bearer {sessionId}'
        )
        return headers

    def headers(self):
        if self._headers is None:
            self._headers = self.login(settings.GUIDESTAR_USERNAME, settings.GUIDESTAR_PASSWORD)
        return self._headers

    def organizations(self, limit=None, regNums=None, filter=True):
        minRegNum = '0'
        done = False
        count = 0
        while not done:
            # print('minRegNum', minRegNum)
            if not regNums:
                _regNums = []
                params = dict(
                    includingInactiveMalkars='false',
                    isDesc='false',
                    sort='regNum',
                    filter=f'servicesCount>0;regNum>{minRegNum}'
                )
                resp = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizations', params=params))
                for row in resp:
                    regNum = row['regNum']
                    _regNums.append(regNum)
                if len(resp) == 0:
                    done = True
                else:
                    newMin = resp[-1]['regNum']
                    assert newMin > minRegNum, '{!r} should be bigger than {!r}'.format(newMin, minRegNum)
                    minRegNum = newMin
            else:
                _regNums = regNums
                done = True
            for regNum in _regNums:
                count += 1
                row = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizations/{regNum}'))
                # print(row)
                if row.get('errorMsg') is not None:
                    errorMsg = row['errorMsg']
                    if errorMsg != 'Not Found':
                        print(f'GUIDESTAR ERROR FETCHING ORGANIZATION {regNum}: {errorMsg}')
                    continue
                yield dict(id=regNum, data=row)
                if limit and count == limit:
                    done = True
                    break
                if count % 25 == 0:
                    print(f'{count} organizations fetched')
    
    def branches(self, regnum):
        if regnum not in self.branch_cache:
            self.branch_cache[regnum] = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizations/{regnum}/branches'))
        return self.branch_cache[regnum]

    def services(self, regnum):
        # params = dict(
        #     filter=f'regNum={regnum}'
        # )
        resp = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizations/{regnum}/services'))
        # resp = requests.get(f'https://www.guidestar.org.il/services/apexrest/api/services', params=params, headers=self.headers(), timeout=self.TIMEOUT).json()
        return resp

    def requests_get(self, *args, **kwargs):
        key = json.dumps([args, kwargs], sort_keys=True)
        data = cache_get(key)
        if data is not None:
            return data
        ret = requests.get(*args, **kwargs, headers=self.headers(), timeout=self.TIMEOUT).json()
        cache_set(key, ret)
        return ret