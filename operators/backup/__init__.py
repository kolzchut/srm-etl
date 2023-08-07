import dataflows as DF

from dataflows_airtable import load_from_airtable
from dataflows_ckan import dump_to_ckan

from conf import settings

TABLES_TO_BACK_UP = [
    settings.AIRTABLE_TAXONOMY_MAPPING_CLICK_TABLE,
    settings.AIRTABLE_TAXONOMY_MAPPING_GUIDESTAR_TABLE,
    settings.AIRTABLE_TAXONOMY_MAPPING_SOPROC_TABLE,
    settings.AIRTABLE_SERVICE_TABLE,
    settings.AIRTABLE_RESPONSE_TABLE,
    settings.AIRTABLE_SITUATION_TABLE,
    settings.AIRTABLE_MANUAL_FIXES_TABLE,
    settings.AIRTABLE_PRESETS_TABLE,
    settings.AIRTABLE_LOCATION_TABLE,
]

def backup():
    DF.Flow(
        *[
            load_from_airtable(
                settings.AIRTABLE_BASE, t, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY
            )
            for t in TABLES_TO_BACK_UP
        ],
        DF.validate(),
        DF.update_package(title='Manual Input Backup', name='backup'),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
        ),            
    ).process()


def operator(*_):
    backup()

if __name__ == "__main__":
    backup()