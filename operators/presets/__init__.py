import dataflows as DF
from dataflows_airtable import load_from_airtable
from dataflows_ckan import dump_to_ckan

from conf import settings

from ..derive.es_utils import dump_to_es_and_delete


def enumerate_rows():
    def func(rows):
        for idx, row in enumerate(rows):
            row['score'] = idx
            yield row
    return DF.Flow(
        DF.add_field('score', 'number', 0),
        func
    )

def operator(*args):
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_PRESETS_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_package(name='presets', title='Presets for Website'),
        DF.update_resource(-1, name='presets', path='presets.csv'),
        enumerate_rows(),
        DF.set_primary_key(['id']),
        DF.select_fields(['id', 'title', 'preset', 'example', 'emergency', 'score']),
        dump_to_es_and_delete(
            indexes=dict(srm__presets=[dict(resource_name='presets')]),
        ),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
            format='json'
        ),
    ).process()