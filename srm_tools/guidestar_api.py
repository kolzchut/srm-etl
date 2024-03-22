import os
import json
import time
import requests
from requests.api import head
import kvfile

from conf import settings

from srm_tools.logger import logger
from srm_tools.debug_cache import cache_get, cache_set

class GuidestarAPI():

    BASE = settings.GUIDESTAR_API
    TIMEOUT = 30
    _headers = None

    def __init__(self):
        self.branch_cache = dict()
        self.org_cache = kvfile.KVFile(location='guidestar_org_cache')
        self.branch_cache = kvfile.KVFile(location='guidestar_branch_cache')
        self.service_cache = kvfile.KVFile(location='guidestar_service_cache')

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

    def organizations(self, limit=None, regNums=None, filter=True, cacheOnly=True):
        count = 0
        if not regNums:
            _regNums = list(self.org_cache.keys())
        else:
            _regNums = regNums
        for regNum in _regNums:
            count += 1
            row = self.org_cache.get(regNum, default=None)
            if row is None:
                continue
            if row.get('errorMsg') is not None:
                errorMsg = row['errorMsg']
                if errorMsg != 'Not Found':
                    print(f'GUIDESTAR ERROR FETCHING ORGANIZATION {regNum}: {errorMsg}')
                continue
            yield dict(id=regNum, data=row)
            if limit and count == limit:
                break
    
    def fetchCaches(self):
        # Orgs
        minRegNum = '0'
        done = False
        count = 0
        while not done:
            params = dict(
                includingInactiveMalkars='false',
                isDesc='false',
                sort='regNum',
                fullObject='true',
                filter=f'servicesCount>0;regNum>{minRegNum}'
            )
            resp = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizations', params=params))
            for row in resp:
                regNum = row['regNum']
                self.org_cache.set(regNum, row)
                count += 1
            if len(resp) == 0:
                done = True
            else:
                newMin = resp[-1]['regNum']
                assert newMin > minRegNum, '{!r} should be bigger than {!r}'.format(newMin, minRegNum)
                minRegNum = newMin
            if count % 250 == 0:
                print(f'{count} organizations fetched')

        # Branches
        minBranchId = None
        done = False
        count = 0
        while not done:
            params = dict(
                isDesc='false',
                sort='branchId',
            )
            if minBranchId is not None:
                params['filter'] = f'branchId>{minBranchId}'
            resp = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizationBranches', params=params))
            for row in resp:
                regNum = row.pop('regNum')
                rec = self.branch_cache.get(regNum, default=[])
                rec.append(row)
                self.branch_cache.set(regNum, rec)
                count += 1
            if len(resp) == 0:
                done = True
            else:
                newMin = resp[-1]['branchId']
                assert minBranchId is None or newMin > minBranchId, '{!r} should be bigger than {!r}'.format(newMin, minBranchId)
                minBranchId = newMin
            if count % 1000 == 0:
                print(f'{count} branches fetched')

        # Services
        minServiceId = None
        done = False
        count = 0
        while not done:
            params = dict(
                isDesc='false',
                sort='serviceId',
            )
            if minServiceId is not None:
                params['filter'] = f'serviceId>{minServiceId}'
            resp = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizationServices', params=params))
            for row in resp:
                regNum = row.pop('regNum')
                rec = self.service_cache.get(regNum, default=[])
                rec.append(row)
                self.service_cache.set(regNum, rec)
                count += 1
            if len(resp) == 0:
                done = True
            else:
                newMin = resp[-1]['serviceId']
                assert minServiceId is None or newMin > minServiceId, '{!r} should be bigger than {!r}'.format(newMin, minServiceId)
                minServiceId = newMin
            if count % 1000 == 0:
                print(f'{count} services fetched')                

    def branches(self, regnum):
        # if regnum not in self.branch_cache:
        #     row = self.org_cache.get(regnum, default=None)
        #     if row is not None and row.get('branchCount') == 0:
        #         branches = []
        #     else:
        #         branches = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizations/{regnum}/branches'))
        #     self.branch_cache[regnum] = branches
        return self.branch_cache.get(regnum, default=[])

    def services(self, regnum):
        # row = self.org_cache.get(regnum, default=None)
        # if row is not None and row.get('servicesCount') == 0:
        #     return []
        # else:
        #     return self.to_json(lambda: self.requests_get(f'{self.BASE}/organizations/{regnum}/services'))
        return self.service_cache.get(regnum, default=[])

    def requests_get(self, *args, **kwargs):
        key = json.dumps([args, kwargs], sort_keys=True)
        data = cache_get(key)
        if data is not None:
            return data
        ret = requests.get(*args, **kwargs, headers=self.headers(), timeout=self.TIMEOUT).json()
        cache_set(key, ret)
        return ret