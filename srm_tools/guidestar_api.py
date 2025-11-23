import os
import json
import time
import requests
from requests.api import head
import kvfile
import shutil

from conf import settings

from srm_tools.logger import logger
from srm_tools.debug_cache import cache_get, cache_set

class GuidestarAPI():

    BASE = settings.GUIDESTAR_API
    TIMEOUT = 30
    _headers = None
    VERIFY_SSL = True

    def __init__(self):
        shutil.rmtree('gacache', ignore_errors=True, onerror=None)
        os.mkdir('gacache')
        self.org_cache = kvfile.KVFile(location='gacache/guidestar_org_cache')
        self.branch_cache = kvfile.KVFile(location='gacache/guidestar_branch_cache')
        self.service_cache = kvfile.KVFile(location='gacache/guidestar_service_cache')
        self.static_language_matchers = {
            'human_situations:language:1_speaking':"human_situations:language:hebrew_speaking",
            'human_situations:language:2_speaking':"human_situations:language:arabic_speaking",
            'human_situations:language:3_speaking': "human_situations:language:russian_speaking",
            'human_situations:language:4_speaking': "human_situations:language:french_speaking",
            'human_situations:language:5_speaking': "human_situations:language:english_speaking",
            'human_situations:language:6_speaking': "human_situations:language:amharic_speaking",
            'human_situations:language:7_speaking': "human_situations:language:spanish_speaking",
            'human_situations:language:8_speaking': "human_situations:language:other_speaking",
        }

    def replace_language_field_in_array_of_object(self, arr):
        for row in arr:
            lang = row.get("language")
            if not lang:
                continue
            fixed_language = self.static_language_matchers.get(lang)
            if fixed_language:
                logger.info(f'Replacing {lang} to {fixed_language}')
                row["language"] = fixed_language



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
        resp = self.to_json(lambda: requests.post(f'{self.BASE}/login', json=dict(username=username, password=password), verify=self.VERIFY_SSL).json())
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
        usedIds = set()
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
                self.branch_cache.set(regNum, [])
                self.service_cache.set(regNum, [])
                count += 1
                assert regNum not in usedIds, f'{regNum} already used'
                usedIds.add(regNum)
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
        usedIds = set()
        while not done:
            params = dict(
                isDesc='false',
                sort='branchId',
            )
            if minBranchId is not None:
                params['filter'] = f'branchId>{minBranchId}'
            resp = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizationBranches', params=params))
            logger.info(f'Fetched {len(resp)} branches from Guidestar API, example: {resp[0] if len(resp) else None}')
            self.replace_language_field_in_array_of_object(resp)
            for row in resp:
                regNum = row.pop('regNum')
                rec = self.branch_cache.get(regNum, default=None) or list()
                rec.append(row)
                self.branch_cache.set(regNum, rec)
                count += 1
                assert row['branchId'] not in usedIds, f'{row["branchId"]} already used'
                usedIds.add(row['branchId'])
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
        usedIds = set()
        while not done:
            params = dict(
                isDesc='false',
                sort='serviceId',
            )
            if minServiceId is not None:
                params['filter'] = f'serviceId>{minServiceId}'
            resp = self.to_json(lambda: self.requests_get(f'{self.BASE}/organizationServices', params=params))
            self.replace_language_field_in_array_of_object(resp)
            for row in resp:
                if row.get('recordType') != 'GreenInfo':
                    continue
                if not row.get('serviceName'):
                    continue
                regNum = row.pop('regNum')
                rec = self.service_cache.get(regNum, default=[])
                rec.append(row)
                self.service_cache.set(regNum, rec)
                count += 1
                assert row['serviceId'] not in usedIds, f'{row["serviceId"]} already used'
                usedIds.add(row['serviceId'])
            if len(resp) == 0:
                done = True
            else:
                newMin = resp[-1]['serviceId']
                assert minServiceId is None or newMin > minServiceId, '{!r} should be bigger than {!r}'.format(newMin, minServiceId)
                minServiceId = newMin
            if count % 1000 == 0:
                print(f'{count} services fetched')    
        for regNum, services in self.service_cache.items():
            if len(services) == 0:
                self.org_cache.delete(regNum)
                self.branch_cache.delete(regNum)

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
        ret = requests.get(*args, **kwargs, headers=self.headers(), timeout=self.TIMEOUT, verify=self.VERIFY_SSL).json()
        cache_set(key, ret)
        return ret