import dataflows as DF
from dataflows_airtable import load_from_airtable

from conf import settings
from operators.mapbox_upload import upload_tileset
from srm_tools.logger import logger

from . import helpers


def geo_data_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/flat_table/datapackage.json'),
        DF.update_package(name='Geo Data'),
        DF.update_resource(['flat_table'], name='geo_data', path='geo_data.csv'),
        # some addresses not resolved to points, and thus they are not useful for the map.
        DF.filter_rows(lambda r: not r['branch_geometry'] is None, resources=['geo_data']),
        DF.join_with_self(
            'geo_data',
            ['service_id', 'response_id', 'branch_id'],
            fields=dict(
                branch_geometry=None,
                response_category=None,
                response_id=None,
                response_name=None,
                organization_id=None,
                organization_name=None,
                branch_id=None,
                branch_name=None,
                service_id=None,
                service_name=None,
                situation_id={'name': 'situation_id', 'aggregate': 'array'},
                situation_name={'name': 'situation_name', 'aggregate': 'array'},
            ),
        ),
        DF.add_field(
            'situations',
            'array',
            lambda r: [
                {'id': id, 'name': name}
                for id, name in zip(r['situation_id'], r['situation_name'])
            ],
        ),
        DF.select_fields(
            [
                'branch_geometry',
                'response_id',
                'response_name',
                'response_category',
                'organization_id',
                'organization_name',
                'branch_id',
                'branch_name',
                'service_id',
                'service_name',
                'situations',
            ],
            resources=['geo_data'],
        ),
        # TODO - When we join with self, it puts the resource into a path under data/
        # this workaround just keeps behaviour same as other dumps we have.
        DF.update_resource(['geo_data'], path='geo_data.csv'),
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/geo_data', format='geojson'),
    )


def push_mapbox_tileset():
    return upload_tileset(
        f'{settings.DATA_DUMP_DIR}/geo_data/geo_data.json',
        'srm-kolzchut.geo-data',
        'SRM Geo Data',
    )


def operator(*_):
    logger.info('Starting Geo Data Flow')

    geo_data_flow().process()
    push_mapbox_tileset()

    logger.info('Finished Geo Data Flow')


if __name__ == '__main__':
    operator(None, None, None)
