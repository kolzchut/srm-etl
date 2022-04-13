import yaml

import requests

import dataflows as DF

from srm_tools.logger import logger
from srm_tools.update_table import airtable_updater

from conf import settings

def handle_tx(s, lang=None):
    # print(lang, s is None, isinstance(s, str), s, s.get('tx', {}).get(lang) or s['source'])
    if s is None:
        return None
    if isinstance(s, str):
        return s
    else:
        return s.get('tx', {}).get(lang) or s['source']


def handle_node(node, breadcrumbs=None):
    if breadcrumbs is None:
        breadcrumbs = []
    name = handle_tx(node['name'], 'he')
    if len(breadcrumbs) > 0:
        record = dict(
            id=node['slug'],
            data=dict(
                name=name,
                description=handle_tx(node.get('description'), 'he'),
                name_en=handle_tx(node['name']),
                description_en=handle_tx(node.get('description')),
                breadcrumbs='/'.join(breadcrumbs[1:]),
            )
        )
        yield record
    for child in node.get('items', []):
        yield from handle_node(child, breadcrumbs + [name])


def fetch_taxonomy(key, languages=('he', )):
    taxonomy = requests.get(settings.OPENELIGIBILITY_YAML_URL).content
    taxonomy = yaml.load(taxonomy, Loader=yaml.SafeLoader)
    root = [t for t in taxonomy if t['slug'] == key][0]
    return DF.Flow(
        handle_node(root),
        DF.set_type('data', type='object', resources=-1),
    )


def operator(*_):
    airtable_updater(
        settings.AIRTABLE_SITUATION_TABLE, 'openeligibility',
        ['name', 'name_en', 'description', 'description_en', 'breadcrumbs'],
        fetch_taxonomy('human_situations'),
        DF.Flow(
            lambda row: row.update(row.get('data', {})),
        )
    )
    airtable_updater(
        settings.AIRTABLE_RESPONSE_TABLE, 'openeligibility',
        ['name', 'name_en', 'description', 'description_en', 'breadcrumbs'],
        fetch_taxonomy('human_services'),
        DF.Flow(
            lambda row: row.update(row.get('data', {})),
        )
    )


if __name__ == '__main__':
    operator()
