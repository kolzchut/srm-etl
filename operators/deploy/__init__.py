from dataclasses import dataclass

import dataflows as DF

from dataflows_airtable import load_from_airtable, dump_to_airtable, AIRTABLE_ID_FIELD

from conf import settings
from srm_tools.logger import logger
from srm_tools.processors import update_mapper
from srm_tools.update_table import airtable_updater
from srm_tools.error_notifier import invoke_on

@dataclass
class DeploySpec:
    table: str
    id_field: str
    copy_fields: list
    add_missing: bool = False


DEPLOY_CONFIG = [
    # Presets
    DeploySpec(settings.AIRTABLE_PRESETS_TABLE, 
               'id', ['title', 'preset', 'example', 'emergency', 'alternative_text'], add_missing=True),
    # Situations and Responses
    DeploySpec(settings.AIRTABLE_SITUATION_TABLE,
               'id', ['synonyms'], add_missing=True),
    DeploySpec(settings.AIRTABLE_RESPONSE_TABLE,
               'id', ['synonyms'], add_missing=True),
    # Manual Location Geo-Tagging
    DeploySpec(settings.AIRTABLE_LOCATION_TABLE,
               'id', ['status', 'provider', 'accuracy',
               'alternate_address', 'resolved_lat', 'resolved_lon',
               'resolved_address', 'resolved_city', 'fixed_lat', 'fixed_lon']),
]

def update_from_source(spec, source_index):
    def func(rows):
        for row in rows:
            id = row[spec.id_field]
            source = source_index.pop(id, None)
            if source is not None:
                if any(row.get(k) != v for k, v in source.items()):
                    row.update(source)
                    yield row
        if spec.add_missing:
            for source in source_index.values():
                yield source
    return func


def run(*_):
    logger.info('Deploy starting')
    for spec in DEPLOY_CONFIG:
        logger.info(f'Deploying {spec.table}')
        select_fields = [f.split(':')[0] for f in spec.copy_fields]
        rename_fields = dict(tuple(f.split(':')) for f in spec.copy_fields if ':' in f)
        source = DF.Flow(
            load_from_airtable(settings.AIRTABLE_ALTERNATE_BASE, spec.table, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.select_fields([spec.id_field] + select_fields),
        ).results(on_error=None)[0][0]
        source = dict((row[spec.id_field], row) for row in source)

        DF.Flow(
            load_from_airtable(settings.AIRTABLE_BASE, spec.table, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.select_fields([spec.id_field, AIRTABLE_ID_FIELD] + select_fields),

            update_from_source(spec, source),

            DF.rename_fields(rename_fields) if rename_fields else None,

            DF.printer(),
            
            dump_to_airtable({
                (settings.AIRTABLE_BASE, spec.table): {
                    'resource-name': spec.table,
                    'typecast': True
                }
            }, settings.AIRTABLE_API_KEY)
        ).process()

    logger.info('Deploy finished')

def operator(*_):
    invoke_on(run, 'Deploy')

if __name__ == '__main__':
    operator(None, None, None)
