import os
import requests
import shutil
import tempfile

import dataflows as DF

def fetch_datagovil(dataset_name, resource_name, temp_file_name):
    dataset = requests.get(f'https://data.gov.il/api/action/package_search?q={dataset_name}').json()['result']['results']
    dataset = [d for d in dataset if d['name'] == dataset_name][0]
    try:
        resource = next(r for r in dataset['resources'] if r['name'] == resource_name)
    except:
        resource = dataset['resources'][0]
    resource = resource['url']
    resource = resource.replace('/e.', '/')
    with open(temp_file_name, 'wb') as outfile:
        r = requests.get(resource, headers={'User-Agent': 'datagov-external-client'}, stream=True)
        if r.status_code == 200:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, outfile)
    print('SAVED from data.gov.il:', dataset_name, 'to', temp_file_name, 'size:', os.path.getsize(temp_file_name))


def fetch_datagovil_datastore(dataset_name, resource_name):
    dataset = requests.get(f'https://data.gov.il/api/action/package_search?q="{dataset_name}"').json()['result']['results']
    dataset = [d for d in dataset if d['name'] == dataset_name][0]
    try:
        resource = next(r for r in dataset['resources'] if r['name'] == resource_name)
    except:
        resource = dataset['resources'][0]
    resource = resource['id']
    URL = f'/api/3/action/datastore_search?resource_id={resource}'
    while URL is not None:
        data = requests.get(f'https://data.gov.il{URL}').json()
        assert data['success']
        records = data.get('result', {}).get('records') or []
        if len(records) == 0:
            break
        print('FETCHED', len(records), 'records from', URL)
        yield from records
        URL = data['result'].get('_links', {}).get('next')
        