from itertools import chain

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
            lambda r: {
                k: v if not k == 'branch_geometry' or v is None else (float(v[0]), float(v[1]))
                for k, v in r.items()
            },
            resources=['geo_data'],
        ),
        helpers.unwind(
            'response_categories',
            'response_category',
            source_delete=False,  # use to generate offsets
            resources=['geo_data'],
        ),
        DF.add_field(
            'offset',
            'array',
            helpers.generate_offset('response_category', 'response_categories'),
            constraints={'maxLength': 2},
            resources=['geo_data'],
        ),
        # some addresses not resolved to points, and thus they are not useful for the map.
        DF.filter_rows(lambda r: not r['branch_geometry'] is None, resources=['geo_data']),
        DF.join_with_self(
            'geo_data',
            ['branch_geometry', 'response_category'],
            fields=dict(
                geometry={'name': 'branch_geometry'},
                response_category=None,
                offset=None,
                situations_at_point={'name': 'situations', 'aggregate': 'array'},
                responses_at_point={'name': 'responses', 'aggregate': 'array'},
                records={'name': 'record', 'aggregate': 'array'},
            ),
        ),
        DF.set_primary_key(['geometry', 'response_category']),
        DF.add_field(
            'situations',
            'array',
            lambda r: sorted(set(s['id'] for s in chain(*r['situations_at_point']))),
            resources=['geo_data'],
        ),
        DF.add_field(
            'responses',
            'array',
            lambda r: sorted(set(s['id'] for s in chain(*r['responses_at_point']))),
            resources=['geo_data'],
        ),
        DF.select_fields(
            [
                'geometry',
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
