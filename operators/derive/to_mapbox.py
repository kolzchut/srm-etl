import dataflows as DF

from conf import settings
from operators.mapbox_upload import upload_tileset
from srm_tools.logger import logger

from . import helpers


def geo_data_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_package(name='Geo Data'),
        DF.update_resource(['card_data'], name='geo_data', path='geo_data.csv'),
        DF.add_field(
            'record',
            'object',
            lambda r: {k: str(v) for k, v in r.items()},
            resources=['geo_data'],
        ),
        DF.add_field(
            'offset',
            'array',
            lambda r: (0, 0),
            constraints={'maxLength': 2},
            resources=['geo_data'],
        ),
        helpers.unwind('response_categories', 'response_category'),
        # some addresses not resolved to points, and thus they are not useful for the map.
        DF.filter_rows(lambda r: not r['branch_geometry'] is None, resources=['geo_data']),
        DF.join_with_self(
            'geo_data',
            ['card_id'],
            fields=dict(
                card_id=None,
                branch_geometry=None,
                offset=None,
                response_category=None,
                outer_situations={'name': 'situations', 'agreggate': 'set'},
                outer_responses={'name': 'responses', 'agreggate': 'set'},
                records={'name': 'record', 'agreggate': 'set'},
            ),
        ),
        DF.set_primary_key(['branch_geometry', 'response_category']),
        DF.rename_fields({'outer_situations': 'situations', 'outer_responses': 'responses'}),
        DF.select_fields(
            [
                'branch_geometry',
                'response_category',
                'offset',
                'situations',
                'responses',
                'records',
            ],
            resources=['geo_data'],
        ),
        # TODO - When we join with self (in some cases??), it puts the resource into a path under data/
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
