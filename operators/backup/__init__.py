import dataflows as DF

from dataflows_airtable import load_from_airtable
from dataflows_ckan import dump_to_ckan
from srm_tools.error_notifier import invoke_on

from conf import settings

TABLES_TO_BACK_UP = [
    (settings.AIRTABLE_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_GUIDESTAR_TABLE),
    (settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_SOPROC_TABLE),
    (settings.AIRTABLE_BASE, settings.AIRTABLE_RESPONSE_TABLE),
    (settings.AIRTABLE_BASE, settings.AIRTABLE_SITUATION_TABLE),
    (settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_MANUAL_FIXES_TABLE),
    (settings.AIRTABLE_BASE, settings.AIRTABLE_PRESETS_TABLE),
    (settings.AIRTABLE_BASE, settings.AIRTABLE_LOCATION_TABLE),
    (settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE),
    (settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_SERVICE_TABLE),
    (settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_BRANCH_TABLE),
]

def run():
    for b, t in TABLES_TO_BACK_UP:
        DF.Flow(
            load_from_airtable(
                b, t, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY
            ),
            DF.validate(),
            DF.update_resource(-1, path=f'{t}.csv'),
            DF.dump_to_path(f'backup/{t}')
        ).process()

    DF.Flow(
        *[
            DF.load(f'backup/{t}/datapackage.json')
            for _, t in TABLES_TO_BACK_UP
        ],
        DF.update_package(title='Manual Input Backup', name='backup'),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
        ),
    ).process()

def backup():
    invoke_on(run, 'Backup')

if __name__ == "__main__":
    backup()
