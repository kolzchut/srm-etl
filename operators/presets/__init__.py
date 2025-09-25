import dataflows as DF
from dataflows_airtable import load_from_airtable
from dataflows_ckan import dump_to_ckan

from conf import settings

from ..derive.es_utils import dump_to_es_and_delete
from srm_tools.error_notifier import invoke_on
from srm_tools.logger import logger

def enumerate_rows():
    def func(rows):
        for idx, row in enumerate(rows):
            row['score'] = idx
            yield row
    return DF.Flow(
        DF.add_field('score', 'number', 0),
        func
    )

def homepage_query(row):
    situation_name = row.get('situation_name')
    response_name = row.get('response_name')
    if situation_name and response_name:
        q = f'{response_name} עבור {situation_name}'
    elif situation_name:
        q = situation_name
    elif response_name:
        q = response_name
    else:
        return
    q = '_'.join(q.split())
    return q

def run(*args):
    logger.info("Deprecated operator called: Presets")
    return

    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_PRESETS_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_package(name='presets', title='Presets for Website'),
        DF.update_resource(-1, name='presets', path='presets.csv'),
        enumerate_rows(),
        DF.set_primary_key(['id']),
        DF.select_fields(['id', 'title', 'preset', 'example', 'emergency', 'alternative_text', 'score']),
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

    DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_HOMEPAGE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_package(name='homepage', title='Homepage Layout'),
        DF.update_resource(-1, name='homepage', path='homepage.csv'),
        enumerate_rows(),
        DF.set_primary_key(['id']),
        DF.set_type('situation_id', type='string', transform=lambda v: v[0] if v else None),
        DF.set_type('response_id', type='string', transform=lambda v: v[0] if v else None),
        DF.set_type('situation_name', type='string', transform=lambda v: v[0] if v else None),
        DF.set_type('response_name', type='string', transform=lambda v: v[0] if v else None),
        DF.add_field('query', 'string', homepage_query),
        DF.filter_rows(lambda row: row.get('group') is not None),
        DF.filter_rows(lambda row: row.get('status') != 'INACTIVE'),
        DF.select_fields(['id', 'group', 'title', 'group_link', 'situation_id', 'response_id', 'score', 'query']),
        dump_to_es_and_delete(
            indexes=dict(srm__homepage=[dict(resource_name='homepage')]),
        ),
        dump_to_ckan(
            settings.CKAN_HOST,
            settings.CKAN_API_KEY,
            settings.CKAN_OWNER_ORG,
            format='json'
        ),
    ).process()

def operator(*_):
    invoke_on(run, 'Presets')
