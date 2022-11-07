import os
from time import time
import requests
from requests.api import head

from conf import settings

class GuidestarAPI():

    BASE = settings.GUIDESTAR_API
    TIMEOUT = 30
    _headers = None

    def __init__(self):
        self.branch_cache = dict()

    def login(self, username, password):
        resp = requests.post(f'{self.BASE}/login', json=dict(username=username, password=password)).json()
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
                    filter=f'branchCount>0;servicesCount>0;regNum>{minRegNum}'
                )
                resp = requests.get(f'{self.BASE}/organizations', params=params, headers=self.headers(), timeout=self.TIMEOUT)
                # print(resp.url)
                resp = resp.json()
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
                row = requests.get(f'{self.BASE}/organizations/{regNum}', headers=self.headers(), timeout=self.TIMEOUT).json()
                # print(row)
                if row.get('errorMsg') is not None:
                    errorMsg = row['errorMsg']
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
            self.branch_cache[regnum] = requests.get(f'{self.BASE}/organizations/{regnum}/branches', headers=self.headers(), timeout=self.TIMEOUT).json()
        return self.branch_cache[regnum]

    def services(self, regnum):
        params = dict(
            filter=f'regNum={regnum}'
        )
        # resp = requests.get(f'{self.BASE}/organizations/{regnum}/services', headers=self.headers()).json()
        resp = requests.get(f'https://www.guidestar.org.il/services/apexrest/api/services', params=params, headers=self.headers(), timeout=self.TIMEOUT).json()
        return resp
