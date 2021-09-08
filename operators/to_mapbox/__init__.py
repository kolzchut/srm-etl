import dataflows as DF
from dataflows.helpers.resource_matcher import ResourceMatcher
from dataflows_airtable import load_from_airtable
from srm_tools.logger import logger
from ..mapbox_upload import upload_tileset
from conf import settings


def filter_airtable_dummies():
    return DF.filter_rows(lambda r: not r['id'] == 'dummy')


def unwind(from_key, to_key, key_transformer=None, resources=None):

    # TODO - errors if to_key does not exist - using add_field below.
    # TODO - from_key and to_key as equal length lists? like zip? See use case below with branch_name and organization_name.
    # TODO - see if this is generic enough for the DF standard lib.
    # TODO - always drop from_key, or, optionally drop?

    def _unwinder(rows):
        for row in rows:
            try:
                iter(row[from_key])
                for value in row[from_key]:
                    row[to_key] = value if key_transformer is None else key_transformer(value)
                    yield row
            except TypeError:
                # no iterable to unwind, likely None. Take the value we have and set it on the to_key.
                row[to_key] = row[from_key] if key_transformer is None else key_transformer(row[from_key])
                yield row

    def func(package):
        matcher = ResourceMatcher(resources, package.pkg)
        yield package.pkg
        for r in package:
            if matcher.match(r.res.name):
                yield _unwinder(r)
            else:
                yield r

    return func


def mapbox_data_flow():
    return DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_RESPONSE_TABLE, settings.AIRTABLE_VIEW),
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW),
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE, settings.AIRTABLE_VIEW),
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_BRANCH_TABLE, settings.AIRTABLE_VIEW),
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW),

        filter_airtable_dummies(),
        DF.update_package(**{'name': 'Mapbox Data'}),
        DF.rename_fields({'__airtable_id': 'pk'}),

        # RESPONSE PRE-PROCESSING
        DF.update_resource(['Responses'], **{'name': 'responses', 'path': 'responses.csv'}),
        DF.select_fields(['pk', 'id', 'name', 'description'], resources=['responses']),

        # SERVICES PRE-PROCESSING
        DF.update_resource(['Services'], **{'name': 'services', 'path': 'services.csv'}),
        DF.filter_rows(lambda r: r['selected'] is True, resources=['services']),
        DF.add_field('organization_pk', 'string', resources=['services']),
        unwind('organizations', 'organization_pk', resources=['services']),
        DF.add_field('response_pk', 'string', resources=['services']),
        unwind('responses', 'response_pk', resources=['services']),
        DF.select_fields(['pk','id', 'selected', 'name', 'organization_pk', 'response_pk'], resources=['services']),

        # ORGANIZATION PRE-PROCESSING
        DF.update_resource(['Organizations'], **{'name': 'organizations', 'path': 'organizations.csv'}),
        DF.add_field('branch_pk', 'string', resources=['organizations']),
        unwind('branches', 'branch_pk', resources=['organizations']),
        DF.select_fields(['pk','id', 'name', 'branch_pk'], resources=['organizations']),

        # BRANCH PRE-PROCESSING
        DF.update_resource(['Branches'], **{'name': 'branches', 'path': 'branches.csv'}),
        DF.add_field('location_pk', 'string', resources=['branches']),
        unwind('location', 'location_pk', resources=['branches']),
        DF.add_field('organization_pk', 'string', resources=['branches']),
        unwind('organization', 'organization_pk', resources=['branches']),
        DF.select_fields(['pk', 'id', 'name', 'location_pk', 'organization_pk'], resources=['branches']),

        # LOCATION PRE-PROCESSING
        DF.update_resource(['Locations'], **{'name': 'locations', 'path': 'locations.csv'}),
        DF.filter_rows(lambda r: any(
            all(r.get(f) for f in fields)
            for fields in [('resolved_lat', 'resolved_lon'), ('fixed_lat', 'fixed_lon')]
        ), resources=['locations']),
        DF.add_field('lat', 'number', lambda r: r.get('fixed_lat') or r['resolved_lat'], resources=['locations']),
        DF.add_field('lon', 'number', lambda r: r.get('fixed_lon') or r['resolved_lon'], resources=['locations']),
        DF.add_field('geometry', 'geopoint', lambda r: [r['lon'], r['lat']], resources=['locations']),
        DF.add_field('address', 'string', lambda r: r.get('resolved_address') or r['id'], resources=['locations']),
        DF.select_fields(['pk', 'geometry', 'address'], resources=['locations']),

        # JOIN TO PRODUCE MAPBOX DATA
        DF.duplicate('services', 'to_mapbox', 'to_mapbox.csv'),
        DF.rename_fields({'name': 'service_name'}, resources=['to_mapbox']),
        DF.join(
            'locations', ['pk'], 'branches', ['location_pk'],
            fields={'geometry': {'name': 'geometry', 'aggregate': 'last'}, 'address': {'name': 'address', 'aggregate': 'last'}},
            source_delete=False,
        ),
        DF.join(
            'organizations', ['pk'], 'branches', ['organization_pk'],
            fields={'organization_name': {'name': 'name', 'aggregate': 'last'}},
            source_delete=False,
        ),
        DF.join(
            # was assuming branch names could differ from org names,
            # but from reviewing the data looks like not.
            # bringing both over anyway because branch_name None means org offers service with no branch.
            'branches', ['organization_pk'], 'to_mapbox', ['organization_pk'],
            fields={
                'branches': {'name': 'name', 'aggregate': 'set'},
                'organizations': {'name': 'organization_name', 'aggregate': 'set'},
                'geometry': {'name': 'geometry', 'aggregate': 'last'},
                'address': {'name': 'address', 'aggregate': 'last'},
            },
            source_delete=False,
        ),
        DF.join(
            'responses', ['pk'], 'to_mapbox', ['response_pk'],
            fields={'response_id': {'name': 'id', 'aggregate': 'last'}, 'response_name': {'name': 'name', 'aggregate': 'last'}},
            source_delete=False
        ),

        # TODO - geometry can still be null. I guess it needs to be filtered out as this data is *only* for the map.

        DF.add_field('branch_name', 'string', resources=['to_mapbox']),
        unwind('branches', 'branch_name', resources=['to_mapbox']),
        DF.add_field('organization_name', 'string', resources=['to_mapbox']),
        unwind('organizations', 'organization_name', resources=['to_mapbox']),
        DF.select_fields(['geometry', 'address', 'response_id', 'response_name', 'service_name', 'branch_name', 'organization_name'], resources=['to_mapbox']),

        # need to drop the other resources - left them here now for debugging.
        DF.dump_to_path(f'{settings.DATA_DUMP_DIR}/to_mapbox', format='geojson'),

        # TODO - looks like I need to de-duplicate, review the source data for duplicates, or check for bugs in my joining process.

        # DF.printer(resources=['to_mapbox']),
    ).process()


def operator(*_):
    logger.info('Starting Data to Mapbox Flow')
    mapbox_data_flow()
    # upload_tileset(f'{settings.DATA_DUMP_DIR}/to_mapbox/to_mapbox.json', 'srm-kolzchut.all-points', 'SRM Mapbox Data')
    return


if __name__ == '__main__':
    operator(None, None, None)
