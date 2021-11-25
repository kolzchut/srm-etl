from itertools import chain
from collections import Counter
import json
import time
import logging
from pathlib import Path
import subprocess

import requests
import boto3
import dataflows as DF

from conf import settings

from dataflows_ckan import dump_to_ckan

from . import helpers
from .es_utils import dump_to_es_and_delete

from srm_tools.logger import logger


def upload_tileset(filename, tileset, name):

    mbtiles = str(Path(filename).with_suffix('.mbtiles'))
    cmd = ['tippecanoe', '-z10', '-o', mbtiles, '-n', name, filename]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf8')
        print(out)
    except subprocess.CalledProcessError as e:
        out = e.output.decode('utf8')
        logger.error(f'Error creating tileset: {out}')
        raise        

    AUTH = dict(access_token=settings.MAPBOX_ACCESS_TOKEN)
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
        mbtiles, creds['bucket'], creds['key']
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


def point_title(r):
    records = r.get('record_objects')
    if len(records) > 1:
        branch = list(set([f['branch_name'] for f in records]))
        if len(branch) == 1:
            return branch[0]
        return '{} שירותים'.format(len(records))  # TODO - multilingual
    else:
        return records[0]['service_name']


def geo_data_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_package(title='Geo Data', name='geo_data'),
        DF.update_resource(['card_data'], name='geo_data', path='geo_data.csv'),
        DF.add_field(
            'record',
            'object',
            lambda r: {
                k: v if not k == 'branch_geometry' or v is None else (float(v[0]), float(v[1]))
                for k, v in r.items()
            },
            resources=['geo_data'],
        ),
        # some addresses not resolved to points, and thus they are not useful for the map.
        DF.join_with_self(
            'geo_data',
            ['point_id'],
            fields=dict(
                branch_geometry={'name': 'branch_geometry'},
                point_id={'name': 'point_id'},
                record_objects={'name': 'record', 'aggregate': 'array'},
            ),
        ),
        DF.set_primary_key(['point_id']),
        DF.add_field(
            'response_categories',
            'array',
            lambda r: [r['response_category'] for r in r['record_objects']],
            resources=['geo_data'],
        ),
        DF.add_field(
            'response_category',
            'string',
            lambda r: Counter(r['response_categories']).most_common(1)[0][0],
            resources=['geo_data'],
            **{'es:keyword': True},
        ),
        DF.add_field(
            'title', 'string', point_title, resources=['geo_data']
        ),
        DF.set_type('record_objects', transform=lambda v, row: helpers.reorder_records_by_category(v, row['response_category'])),
        DF.add_field(
            'records',
            'string',
            lambda r: json.dumps(r['record_objects']),
            resources=['geo_data'],
            **{'es:itemType': 'string', 'es:index': False}
        ),
        DF.add_field(
            'service_count',
            'integer',
            lambda r: len(r['record_objects']),
            resources=['geo_data'],
        ),
        DF.select_fields(
            [
                'branch_geometry',
                'response_category',
                'records',
                'title',
                'point_id',
                'service_count',
            ],
            resources=['geo_data'],
        ),
        # TODO - When we join with self (in some cases??), it puts the resource into a path under data/
        # this workaround just keeps behaviour same as other dumps we have.
        DF.update_resource(['geo_data'], path='geo_data.csv'),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/geo_data', format='geojson'),
    )


def points_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_package(title='Points Data', name='points_data'),
        DF.update_resource(['card_data'], name='points', path='points.csv'),
        DF.set_primary_key(['card_id']),
        DF.select_fields(
            [
                'branch_geometry',
                'response_categories',
                'response_category',
                'card_id',
                'point_id',
                'situation_ids',
                'response_ids',
            ],
            resources=['points'],
        ),
        DF.add_field('score', 'number', 10, resources=['points']),
        # Save mapbox data to ES and CKAN
        dump_to_es_and_delete(
            indexes=dict(srm__points=[dict(resource_name='points')]),
        ),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
            force_format=False
        ),

        # Generate Cluster dataset
        DF.select_fields(['branch_geometry', 'response_categories', 'point_id']),
        DF.update_package(name='geo_data_clusters', title='Geo Data - For Clusters'),
        DF.update_resource(['points'], path='geo_data.geojson'),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/geo_data_clusters', force_format=False),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
            force_format=False
        ),
    )



def push_mapbox_tileset():
    return upload_tileset(
        f'{settings.DATA_DUMP_DIR}/geo_data/geo_data.geojson',
        'srm-kolzchut.geo-data',
        'SRM Geo Data',
    )


def operator(*_):
    logger.info('Starting Geo Data Flow')

    geo_data_flow().process()
    push_mapbox_tileset()
    points_flow().process()
    logger.info('Finished Geo Data Flow')


if __name__ == '__main__':
    operator(None, None, None)
