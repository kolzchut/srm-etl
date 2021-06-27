import os
import requests
from requests.api import head

BASE = 'https://www.guidestar.org.il/services/apexrest/api'

def login(user, password):
    sessionId = requests.post(f'{BASE}/login', json=dict(user=user, password=password)).json()['sessionId']
    headers = dict(
        Authorization=f'Bearer {sessionId}'
    )
    return headers


def headers():
    global _headers
    if _headers is None:
        _headers = login(os.environ['GUIDESTAR_USERNAME'], os.environ['GUIDESTAR_PASSWORD'])

