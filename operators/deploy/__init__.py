from dataclasses import dataclass

import dataflows as DF

from dataflows_airtable import load_from_airtable, dump_to_airtable, AIRTABLE_ID_FIELD

from conf import settings
from srm_tools.logger import logger

@dataclass
class DeploySpec:
    table: str
    id_field: str
    copy_fields: list
    add_missing: bool = False


DEPLOY_CONFIG = [
    # Presets
    DeploySpec(settings.AIRTABLE_PRESETS_TABLE, 
               'id', ['title'], add_missing=True),
    # Situations and Responses
    DeploySpec(settings.AIRTABLE_SITUATION_TABLE,
               'id', ['synonyms'], add_missing=True),
    DeploySpec(settings.AIRTABLE_RESPONSE_TABLE,
               'id', ['synonyms'], add_missing=True),
    # Organization Short Names
    DeploySpec(settings.AIRTABLE_ORGANIZATION_TABLE,
               'id', ['short_name']),
    # Service Manual Tagging
    DeploySpec(settings.AIRTABLE_SERVICE_TABLE,
               'id', ['responses_manual_ids:responses_manual', 'situations_manual_ids:situations_manual']),
    # Manual Location Geo-Tagging
    DeploySpec(settings.AIRTABLE_LOCATION_TABLE,
               'id', ['alternate_address', 'resolved_lat', 'resolved_lon', 'resolved_address', 'resolved_city', 'fixed_lat', 'fixed_lon']),
]

def update_from_source(spec, source_index):
    def func(rows):
        for row in rows:
            id = row[spec.id_field]
            source = source_index.get(id)
            if source is not None:
                if any(row.get(k) != v for k, v in source.items()):
                    row.update(source)
                    yield row
    return func


def operator(*_):
    logger.info('Deploy starting')
    for spec in DEPLOY_CONFIG:
        logger.info(f'Deploying {spec.table}')
        select_fields = [f.split(':')[0] for f in spec.copy_fields]
        rename_fields = dict(tuple(f.split(':')) for f in spec.copy_fields if ':' in f)
        source = DF.Flow(
            load_from_airtable(settings.AIRTABLE_ALTERNATE_BASE, spec.table, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.select_fields([spec.id_field] + select_fields),
        ).results()[0][0]
        source = dict((row.pop(spec.id_field), row) for row in source)

        DF.Flow(
            load_from_airtable(settings.AIRTABLE_BASE, spec.table, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.select_fields([spec.id_field, AIRTABLE_ID_FIELD] + select_fields),

            update_from_source(spec, source),

            DF.rename_fields(rename_fields) if rename_fields else None,

            DF.delete_fields([spec.id_field]),

            DF.printer(),
            
            dump_to_airtable({
                (settings.AIRTABLE_BASE, spec.table): {
                    'resource-name': spec.table,
                    'typecast': True
                }
            }, settings.AIRTABLE_API_KEY)
        ).process()

    logger.info('Deploy done')

if __name__ == '__main__':
    operator(None, None, None)