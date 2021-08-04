import os
import requests
from requests.api import head

class GuidestarAPI():

    BASE = 'https://www.guidestar.org.il/services/apexrest/api'
    _headers = None

    def login(self, username, password):
        resp = requests.post(f'{self.BASE}/login', json=dict(username=username, password=password)).json()
        sessionId = resp['sessionId']
        headers = dict(
            Authorization=f'Bearer {sessionId}'
        )
        return headers

    def headers(self):
        if self._headers is None:
            self._headers = self.login(os.environ['GUIDESTAR_USERNAME'], os.environ['GUIDESTAR_PASSWORD'])
        return self._headers

    def organizations(self, limit=None):
        minRegNum = '0'
        done = False
        count = 0
        while not done:
            # print('minRegNum', minRegNum)
            params = dict(
                includingInactiveMalkars='false',
                isDesc='false',
                sort='regNum',
                filter=f'branchCount>0;regNum>{minRegNum}'
            )
            resp = requests.get(f'{self.BASE}/organizations', params=params, headers=self.headers())
            # print(resp.url)
            resp = resp.json()
            for row in resp:
                count += 1
                yield row
                if limit and count == limit:
                    done = True
                    break
            if len(resp) == 0:
                done = True
            else:
                newMin = resp[-1]['regNum']
                assert newMin > minRegNum, '{!r} should be bigger than {!r}'.format(newMin, minRegNum)
                minRegNum = newMin
    
    def branches(self, regnum):
        resp = requests.get(f'{self.BASE}/organizations/{regnum}/branches', headers=self.headers()).json()
        return resp
