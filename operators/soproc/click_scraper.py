import json
import requests
import codecs
import pathlib

import bleach
import dataflows as DF
from dataflows_airtable import load_from_airtable

from conf import settings

KEEP_FIELDS = ['cat', 'Name']
DT_SUFFIXES = dict((k, i) for i, k in enumerate(['', 'i', 'ss', 't', 's', 'base64', 'f', 'is']))
NO_LISTS = ['Short_Description']
SELECT_FIELDS = {
    'id': 'catalog_number',
    'data_sources': 'data_sources',
    'urls': 'urls',

    # 'DisplayName': '',

    # 'Administration': 'department',
    'parent_group_name': 'service_group',
    'group_name': 'unit',
    'FamilyName': 'name',

    'Service_Purpose': 'purpose',
    'Short_Description': 'description',
    'Description': 'details',

    'Normative_Source': 'normative_source',

    'Domin': 'service_subject',
    # 'Type': 'service_type',
    'Target_Population_A': 'target_populations_level_1',
    'Target_Population': 'target_populations_level_2',

    'Age_Minimum': 'age_min',
    'Age_Maximum': 'age_max',

    'Target_Community': 'target_community_text',
    'Duration_of_Service': 'service_duration_text',

    'Deducitable': 'payment_required',
    'Deductible': 'payment_details',
    # 'Relationship_Type', # TODO: See if useful
    
    # 'Service_Status': 'service_status',
    # 'Service_Channels': 'delivery_channels',
    # 'Right_or_Service', # TODO: See if useful
    # 'Program_Activation_Model': 'internal_operation_model',
    # 'location_type', # TODO: Probably not useful
    
    'Implementaion_Process': 'implementation_details',
    
    'Link_to_Kolzchut': 'link_to_kolzchut',
    'Link_to_Molsa': 'link_to_molsa',
    'Link_to_TAAS': 'link_to_taas',

    'Causes_Referes': 'causes_referes',
    'Location': 'location',
    'Informational_Notes': 'notes',
}
DEDUCTIBLE_TYPE = {
    'אינו כרוך בהשתתפות עצמית': 'no',
    'בחלק מהמקרים תתכן השתתפות עצמית': 'sometimes',
    'כרוך בהשתתפות עצמית': 'yes'
}


def decode_and_clean():
    def func(row):
        for k, v in row.items():
            if isinstance(v, str):
                try:
                    v = codecs.decode(v.encode('ascii'), 'base64').decode('utf8')
                except:
                    pass
                v = bleach.clean(v, strip=True).replace('&nbsp;', ' ').replace('\xa0', ' ').replace('\r', '').strip()
                if v == 'NULL':
                    v = None
                row[k] = v
    return func


def filter_results():
    return DF.Flow(
        DF.filter_rows(lambda row: row.get('lang_code') == 'he'),
        DF.set_type('type', type='integer', on_error=DF.schema_validator.drop),
        DF.filter_rows(lambda row: row.get('type') == 1),
        DF.filter_rows(lambda row: row.get('group_id') is not None),
        DF.filter_rows(lambda row: row.get('distribution_channel') is not None and row.get('distribution_channel')[0] == 1),
    )

def fetch_from_taxonomy(taxonomy, field):
    def func(r):
        tags = r['tags'] or []
        ret = set()
        for t in tags:
            rec = taxonomy[t]
            val = rec.get(field) or []
            ret.update(val)
        return sorted(ret)
    return func


def scrape_click():
    try:
        docs = json.load(open('click-cache.json'))
    except:
        try:
            docs = requests.get(settings.CLICK_API, headers={'User-Agent': 'kz-data-reader'})
            docs = docs.json().get('response').get('docs')
            json.dump(docs, open('click-cache.json', 'w'))
            print('SCRAPING CLICK', len(docs))
        except:
            docs = json.load(pathlib.Path(__file__).with_name('click-cache-backup.json').open())
    all_keys = set()
    for doc in docs:
        all_keys.update(k for k, v in doc.items() if v)
    config = dict()
    for k in all_keys:
        if k in KEEP_FIELDS:
            config[k] = [[k, k, '']]
        else:
            suffix = k.split('_')[-1]
            if suffix in DT_SUFFIXES:
                prefix = k[:-len(suffix)-1]
                config.setdefault(prefix, []).append((prefix, k, suffix))
    concat_fields = dict()
    for k, suffixes in config.items():
        suffixes = sorted(suffixes, key=lambda s: DT_SUFFIXES[s[2]])
        while k in NO_LISTS and DT_SUFFIXES[suffixes[0][2]] < 3:
            suffixes.pop(0)
        prefix, k, _ = suffixes[0]
        # prefix = prefix.lower()
        concat_fields[prefix] = [k] if prefix != k else []

    docs = (
        dict((k, doc.get(k)) for k in all_keys)
        for doc in docs
    )
    # print(next(docs))
    
    # taxonomy = DF.Flow(
    #     load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_CLICK_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    # ).results()[0][0]
    # taxonomy = dict(
    #     (r.pop('name'), r) for r in taxonomy
    # )

    records = DF.Flow(
        docs,
        DF.concatenate(concat_fields),
        DF.update_resource(-1, name='click'),
        decode_and_clean(),
        filter_results(),
        DF.add_field('data_sources', 'string', None),
        DF.add_field('urls', 'string', None),
        DF.select_fields(list(SELECT_FIELDS.keys())),
        DF.rename_fields(SELECT_FIELDS),
        DF.set_type('details',
            transform=lambda _, row: ''.join('<p>{}</p>'.format(row[f].strip()) for f in [
                'description', 'details', 'implementation_details','target_community_text', 'service_duration_text'
            ] if isinstance(row.get(f), str))
        ),
        DF.add_field('tags', 'array', 
            lambda row: (
                ['age-{age_min}-{age_max}'.format(**row)] +
                (row.get('target_populations_level_1') or []) +
                (row.get('target_populations_level_2') or []) +
                (row.get('service_subject') or [])
            )
        ),
        # DF.set_type('delivery_channels', type='array', transform=lambda v: v.split('|') if v else []),
        DF.set_type('name', type='string', transform=lambda v: ''.join(v).strip()),
        DF.set_type('payment_required', type='string', transform=lambda v: DEDUCTIBLE_TYPE.get(v)),
        DF.add_field('links', 'array', lambda r: list(filter(None, [v for k, v in r.items() if k.startswith('link_')]))),
        DF.select_fields(['catalog_number', 'name', 'description', 'details', 'payment_required', 'payment_details', 'data_sources', 'urls']),
        # DF.printer()
    ).results()[0][0]
    return dict((r['catalog_number'], r) for r in records)


if __name__ == '__main__':
    sc = scrape_click()
    print(len(sc))
    import pprint
    # pprint.pprint(sc.keys())
    # docs = json.load(open('click-cache.json'))
    # pprint.pprint([d for d in docs if d.get('product_id_i')==143][0])
    pprint.pprint(sc['143'])
    # all_tags = [t for v in sc.values() for t in v['tags']]
    # with open('tags', 'w') as t:
    #     t.write('\n'.join(sorted(set(all_tags))))