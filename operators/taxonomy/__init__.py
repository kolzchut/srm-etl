import yaml
from itertools import chain

import requests

import dataflows as DF

from srm_tools.logger import logger
from srm_tools.update_table import airtable_updater
from srm_tools.error_notifier import invoke_on

from conf import settings

def handle_tx(s, lang=None):
    # print(lang, s is None, isinstance(s, str), s, s.get('tx', {}).get(lang) or s['source'])
    if s is None:
        return None
    if isinstance(s, str):
        return s
    else:
        return s.get('tx', {}).get(lang) or s['source']


def handle_node(node, breadcrumbs=None, renames=[]):
    if breadcrumbs is None:
        breadcrumbs = []
    name = handle_tx(node['name'], 'he')
    if len(breadcrumbs) > 0:
        slug = node['slug']
        for src, dst in renames:
            slug = slug.replace(src, dst)
        record = dict(
            id=slug,
            data=dict(
                name=name,
                description=handle_tx(node.get('description'), 'he'),
                name_en=handle_tx(node['name']),
                description_en=handle_tx(node.get('description')),
                breadcrumbs='/'.join(breadcrumbs[1:]),
                pk=node.get('pk')
            )
        )
        yield record
    for child in node.get('items', []):
        yield from handle_node(child, breadcrumbs + [name], renames=renames)


INTERNAL_RESPONSES = [
    dict(
        id='human_services:internal_emergency_services',
        data=dict(
            name='שירותים למצב החירום',
            description='',
            name_en='State of Emergency',
            description_en='',
            breadcrumbs='שירותים למצב החירום',
        )
    ),
    dict(
        id='human_services:place',
        data=dict(
            name='מקומות',
            description='',
            name_en='Places',
            description_en='',
            breadcrumbs='מקומות',
        )
    )
]


def fetch_taxonomy(keys, extra=[], renames=[]):
    taxonomy = requests.get(settings.OPENELIGIBILITY_YAML_URL).content
    taxonomy = yaml.load(taxonomy, Loader=yaml.SafeLoader)
    roots = [[t for t in taxonomy if t['slug'] == key][0] for key in keys]
    return DF.Flow(
        chain(*(handle_node(root, renames=renames) for root in roots), extra),
        DF.set_type('data', type='object', resources=-1),
    )


def run(*_):
    airtable_updater(
        settings.AIRTABLE_SITUATION_TABLE, 'openeligibility',
        ['name', 'name_en', 'description', 'description_en', 'breadcrumbs', 'pk'],
        fetch_taxonomy(['human_situations']),
        DF.Flow(
            lambda row: row.update(row.get('data', {})),
        )
    )
    airtable_updater(
        settings.AIRTABLE_RESPONSE_TABLE, 'openeligibility',
        ['name', 'name_en', 'description', 'description_en', 'breadcrumbs', 'pk'],
        fetch_taxonomy(['human_services', 'human_places'], INTERNAL_RESPONSES, [('human_places:', 'human_services:place:')]),
        DF.Flow(
            lambda row: row.update(row.get('data', {})),
        )
    )

def operator(*_):
    invoke_on(run, 'Taxonomies')

if __name__ == '__main__':
    operator()
