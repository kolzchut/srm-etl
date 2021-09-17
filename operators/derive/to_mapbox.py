import dataflows as DF
from dataflows_airtable import load_from_airtable

from conf import settings
from operators.mapbox_upload import upload_tileset
from srm_tools.logger import logger

from . import helpers


def geo_data_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/table_data/datapackage.json'),
        DF.update_package(name='Geo Data'),
        DF.update_resource(['table_data'], name='geo_data', path='geo_data.csv'),
        DF.join_with_self(
            'geo_data',
            ['response_id', 'branch_geometry'],
            fields=dict(
                branch_geometry=None,
                response_id=None,
                response_name=None,
                organization_id={'name': 'organization_id', 'aggregate': 'set'},
                organization_name={'name': 'organization_name', 'aggregate': 'set'},
                branch_id={'name': 'branch_id', 'aggregate': 'set'},
                branch_name={'name': 'branch_name', 'aggregate': 'set'},
                service_id={'name': 'service_id', 'aggregate': 'set'},
                service_name={'name': 'service_name', 'aggregate': 'set'},
                situation_id={'name': 'situation_id', 'aggregate': 'set'},
                situation_name={'name': 'situation_name', 'aggregate': 'set'},
            ),
        ),
        DF.add_field(
            'organizations',
            'array',
            lambda r: [
                {'id': vals[0], 'name': vals[1]}
                for vals in zip(r['organization_id'], r['organization_name'])
            ],
        ),
        DF.add_field(
            'branches',
            'array',
            lambda r: [
                {'id': vals[0], 'name': vals[1]} for vals in zip(r['branch_id'], r['branch_name'])
            ],
        ),
        DF.add_field(
            'services',
            'array',
            lambda r: [
                {'id': vals[0], 'name': vals[1]}
                for vals in zip(r['service_id'], r['service_name'])
            ],
        ),
        DF.add_field(
            'situations',
            'array',
            lambda r: [
                {'id': vals[0], 'name': vals[1]}
                for vals in zip(r['situation_id'], r['situation_name'])
            ],
        ),
        # some addresses not resolved to points, and they are not useful for the map.
        DF.filter_rows(lambda r: not r['branch_geometry'] is None),
        DF.select_fields(
            [
                'branch_geometry',
                'response_id',
                'response_name',
                'organizations',
                'branches',
                'services',
                'situations',
            ]
        ),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/geo_data', format='geojson'),
    )


def push_mapbox_tileset():
    return upload_tileset(
        f'{settings.DATA_DUMP_DIR}/geo_data/data/geo_data.json',
        'srm-kolzchut.all-points',
        'SRM Geo Data',
    )


def operator(*_):
    logger.info('Starting Geo Data Flow')

    geo_data_flow().process()
    push_mapbox_tileset()

    logger.info('Finished Geo Data Flow')


if __name__ == '__main__':
    operator(None, None, None)
