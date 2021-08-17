import os
from dataflows_airtable import load_from_airtable
import requests
import boto3
import time
import dataflows as DF

from srm_tools.logger import logger

from conf import settings


AUTH = dict(access_token=settings.MAPBOX_ACCESS_TOKEN)


def fetch_tilesets():
    return requests.get(settings.MAPBOX_LIST_TILESETS, params=AUTH).json()


def upload_tileset(filename, tileset, name):
    creds = requests.get(settings.MAPBOX_UPLOAD_CREDENTIALS, params=AUTH).json()
    print(creds, AUTH)
    s3_client = boto3.client(
        's3',
        aws_access_key_id=creds['accessKeyId'],
        aws_secret_access_key=creds['secretAccessKey'],
        aws_session_token=creds['sessionToken'],
        region_name='us-east-1',
    )
    s3_client.upload_file(
        filename, creds['bucket'], creds['key']
    )
    data = dict(
        tileset=tileset,
        url=creds['url'],
        name=name
    )
    upload = requests.post(settings.MAPBOX_CREATE_UPLOAD, params=AUTH, json=data).json()
    print(upload)
    assert not upload.get('error')
    while True:
        status = requests.get(settings.MAPBOX_UPLOAD_STATUS + upload['id'], params=AUTH).json()
        assert not status.get('error')
        print('{complete} / {progress}'.format(**status))
        if status['complete']:
            break
        time.sleep(10)

def unwind_titles():
    def func(rows):
        for row in rows:
            for title in row['names'] or []:
                row['title'] = title
                yield row

    return func


def upload_locations():
    print('PREPARING ALL Locations')

    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE, settings.AIRTABLE_VIEW),
        DF.update_resource(-1, **{'name': 'locations', 'path': 'all-points.csv'}),
        DF.filter_rows(lambda r: any(
            all(r.get(f) for f in fields)
            for fields in [('resolved_lat', 'resolved_lon'), ('fixed_lat', 'fixed_lon')]
        )),
        DF.filter_rows(lambda r: r['status'] == 'VALID'),
        DF.add_field('lat', 'number', lambda r: r.get('fixed_lat') or r['resolved_lat']),
        DF.add_field('lon', 'number', lambda r: r.get('fixed_lon') or r['resolved_lon']),
        DF.add_field('geometry', 'geopoint', lambda r: [r['lon'], r['lat']]),
        DF.add_field('title', 'string'),
        unwind_titles(),
        DF.select_fields(['geometry', 'title']),
        DF.dump_to_path('geojson', format='geojson'),
        DF.printer()
    ).process()
    print('UPLOADING TO MAPBOX')
    upload_tileset('geojson/all-points.json', 'srm-kolzchut.all-points', 'All Points')


def operator(name, params, pipeline):
    upload_locations()
    

if __name__ == '__main__':
    operator(None, None, None)
