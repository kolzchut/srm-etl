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

    mbtiles = Path(filename).with_suffix('.mbtiles')
    if mbtiles.exists():
        mbtiles.unlink()
    mbtiles = str(mbtiles)
    layer_name = tileset.split('.')[-1].replace('-', '_')
    cmd = ['tippecanoe', '-ai', '-B6', '-z10', '-o', mbtiles, '-n', name, '-l', layer_name, filename]
    try:
        out = subprocess.check_output(cmd, stderr=subprocess.STDOUT).decode('utf8')
        print(out)
    except subprocess.CalledProcessError as e:
        out = e.output.decode('utf8')
        logger.error(f'Error creating tileset: {out}')
        raise        

    AUTH = dict(access_token=settings.MAPBOX_ACCESS_TOKEN)
    creds = requests.get(settings.MAPBOX_UPLOAD_CREDENTIALS, params=AUTH).json()
    # print(creds, AUTH)
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
    print('UPLOAD:', upload)
    assert not upload.get('error')
    while True:
        status = requests.get(settings.MAPBOX_UPLOAD_STATUS + upload['id'], params=AUTH).json()
        assert not status.get('error')
        print('{complete} / {progress}'.format(**status))
        if status['complete']:
            break
        time.sleep(10)


def branches(r):
    records = r.get('records')
    return list([f['branch_operating_unit'] or f['organization_short_name'] or f['organization_name'] for f in records])


def point_title(r, full=False):
    max_len = 20
    branch = branches(r)
    branch = Counter(branch)
    bn = branch.most_common(1)[0][0]
    if not full and len(bn) > max_len:
        bn = bn[:max_len] + '…'
    if len(branch) > 1:
        bn += '  +{}'.format(len(branch) - 1)
    else:
        if not r.get('branch_location_accurate', True):
            bn += '*'
    return bn


def preprocess_field(k, v):
    if v is None:
        return None
    if k == 'branch_geometry':
        return (float(v[0]), float(v[1]))
    return v


def geo_data_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_package(title='Full Point Data', name='geo_data'),
        DF.update_resource(['card_data'], name='geo_data', path='geo_data.csv'),
        DF.filter_rows(lambda r: r['branch_geometry'] is not None),
        DF.add_field(
            'record',
            'object',
            lambda r: {
                k: preprocess_field(k, v)
                for k, v in r.items()
            },
            resources=['geo_data'],
        ),
        # some addresses not resolved to points, and thus they are not useful for the map.
        DF.join_with_self(
            'geo_data',
            ['point_id'],
            fields=dict(
                branch_geometry=None,
                branch_location_accurate=dict(aggregate='max'),
                point_id=None,
                records={'name': 'record', 'aggregate': 'array'},
            ),
        ),
        DF.set_primary_key(['point_id']),
        DF.add_field(
            'response_categories',
            'array',
            lambda r: [r['response_category'] for r in r['records']],
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
        DF.add_field(
            'full_title', 'string', lambda r: point_title(r, True), resources=['geo_data']
        ),
        # DF.set_type(
        #     'records',
        #     transform=lambda v, row: helpers.reorder_records_by_category(v, row['response_category']),
        #     **{'es:index': False, 'es:itemType': 'object'},
        # ),
        DF.add_field(
            'service_count',
            'integer',
            lambda r: len(r.get('records') or []),
            resources=['geo_data'],
        ),
        DF.add_field(
            'branch_count',
            'integer',
            lambda r: len(set(branches(r))),
            resources=['geo_data'],
        ),
        DF.add_field(
            'card_id',
            'string',
            lambda r: r['records'][0]['card_id'] if r['service_count'] == 1 else None,
            resources=['geo_data'],
        ),
        DF.select_fields(
            [
                'branch_geometry',
                'branch_location_accurate',
                'response_category',
                # 'records',
                'title',
                'full_title',
                'point_id',
                'service_count',
                'branch_count',
                'card_id',
            ],
            resources=['geo_data'],
        ),
        DF.add_field('score', 'number', 10, resources=['geo_data']),
        # dump_to_es_and_delete(
        #     indexes=dict(srm__geo_data=[dict(resource_name='geo_data')]),
        # ),
        DF.update_resource(['geo_data'], path='geo_data.csv'),
        # dump_to_ckan(
        #     settings.CKAN_HOST,
        #     settings.CKAN_API_KEY,
        #     settings.CKAN_OWNER_ORG,
        #     force_format=False
        # ),
        DF.delete_fields(['score'], resources=['geo_data']),
        DF.duplicate('geo_data', target_name='geo_data_inaccurate', target_path='geo_data_inaccurate.csv'),
        DF.filter_rows(lambda r: r['branch_location_accurate'] is True, resources=['geo_data']),
        DF.filter_rows(lambda r: r['branch_location_accurate'] is False, resources=['geo_data_inaccurate']),        
        # DF.set_type(
        #     'records',
        #     type='string',
        #     transform=lambda v: json.dumps([dict(card_id=vv['card_id']) for vv in v]),
        #     resources=['geo_data'],
        # ),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/geo_data', format='geojson'),
    )


def points_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_package(title='Points Data', name='points_data'),
        DF.update_resource(['card_data'], name='points', path='points.csv'),
        DF.set_primary_key(['card_id']),
        DF.filter_rows(lambda r: r['branch_geometry'] is not None),
        DF.select_fields(
            [
                'branch_geometry',
                'response_categories',
                'response_category',
                'card_id',
                'point_id',
                'situation_ids',
                'response_ids',
                'organization_id',
            ],
            resources=['points'],
        ),
        DF.add_field('score', 'number', 10, resources=['points']),
        # Save mapbox data to ES and CKAN
        dump_to_es_and_delete(
            indexes=dict(srm__points=[dict(resource_name='points')]),
        ),
        # dump_to_ckan(
        #     settings.CKAN_HOST,
        #     settings.CKAN_API_KEY,
        #     settings.CKAN_OWNER_ORG,
        #     force_format=False
        # ),

        # Generate Cluster dataset
        DF.select_fields(['branch_geometry', 'response_categories', 'point_id', 'card_id']),
        DF.update_package(name='geo_data_clusters', title='Geo Data - For Clusters'),
        DF.update_resource(['points'], path='geo_data.geojson'),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/geo_data_clusters', force_format=False),
        # dump_to_ckan(
        #     settings.CKAN_HOST,
        #     settings.CKAN_API_KEY,
        #     settings.CKAN_OWNER_ORG,
        #     force_format=False
        # ),
    )



def push_mapbox_tileset():
    upload_tileset(
        f'{settings.DATA_DUMP_DIR}/geo_data/geo_data.geojson',
        settings.MAPBOX_TILESET_ID,
        settings.MAPBOX_TILESET_NAME,
    )
    upload_tileset(
        f'{settings.DATA_DUMP_DIR}/geo_data/geo_data_inaccurate.geojson',
        settings.MAPBOX_TILESET_INACCURATE_ID,
        settings.MAPBOX_TILESET_INACCURATE_NAME,
    )


def operator(*_):
    logger.info('Starting Geo Data Flow')

    geo_data_flow().process()
    push_mapbox_tileset()
    points_flow().process()

    logger.info('Finished Geo Data Flow')


if __name__ == '__main__':
    operator(None, None, None)
