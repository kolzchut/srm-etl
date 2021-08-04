import json
from datapackage import resource

import requests

import dataflows as DF

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

KEEP_FIELDS = ['cat', 'Name']
DT_SUFFIXES = dict((k, i) for i, k in enumerate(['', 'i', 'ss', 't', 's', 'base64', 'f', 'is']))
SELECT_FIELDS = {
    'id': 'id',
    'page_url': 'page_url',

    # 'DisplayName': '',

    'Administration': 'department',
    'parent_group_name': 'service_group',
    'group_name': 'unit',
    'FamilyName': 'title',

    'Service_Purpose': 'description',
    'Short_Description': 'subtitle',
    'Description': 'details',

    'Naming_Outputs': 'tags',

    'Domin': 'service_subject',
    'Type': 'service_type',
    'Target_Population_A': 'target_populations_level_1',
    'Target_Population': 'target_populations_level_2',

    'Age_Minimum': 'age_min',
    'Age_Maximum': 'age_max',

    'Target_Community': 'target_community_text',
    'Duration_of_Service': 'service_duration_text',

    'Deducitable': 'deductible',
    'Deductible': 'deductible_details',
    # 'Relationship_Type', # TODO: See if useful
    
    'Service_Status': 'service_status',
    'Service_Channels': 'delivery_channels',
    # 'Right_or_Service', # TODO: See if useful
    'Program_Activation_Model': 'internal_operation_model',
    # 'location_type', # TODO: Probably not useful
    
    'Implementaion_Process': 'implementation_details',
    
    'Link_to_Kolzchut': 'link_to_kolzchut',

    AIRTABLE_ID_FIELD: AIRTABLE_ID_FIELD
}
DEDUCTIBLE_TYPE = {
    'אינו כרוך בהשתתפות עצמית': 'no',
    'בחלק מהמקרים תתכן השתתפות עצמית': 'sometimes',
    'כרוך בהשתתפות עצמית': 'yes'
}


def scrape_click():
    print('hi')
    try:
        docs = json.load(open('click-cache.json'))
    except:
        docs = requests.get('https://clickrevaha-sys.molsa.gov.il/api/solr?rows=1000').json().get('response').get('docs')
        json.dump(docs, open('click-cache.json', 'w'))

    print(len(docs))
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
    print(config['type'])
    concat_fields = dict()
    for k, suffixes in config.items():
        prefix, k, _ = sorted(suffixes, key=lambda s: DT_SUFFIXES[s[2]])[0]
        # prefix = prefix.lower()
        concat_fields[prefix] = [k] if prefix != k else []

    docs = (
        dict((k, doc.get(k)) for k in all_keys)
        for doc in docs
    )
    # print(next(docs))
    
    return DF.Flow(
        docs,
        DF.concatenate(concat_fields, resources=-1),
    )
    # all_keys = all_keys - set(base64_keys)
    # print(all_keys)
    # print(base64_keys)
    # for doc in docs:
    #     ret = dict((k, doc.get(k)) for k in all_keys)
    #     ret.update(dict(
    #         (k[:-7],
    #          codecs.decode(doc.get(k).encode('ascii'), 'base64').decode('utf8')
    #          if doc.get(k)
    #          else None
    #         ) for k in base64_keys)
    #     )
    #     ret = dict((k, ret[k]) for k in sorted(ret.keys()))
    #     yield ret

def remove_nbsp():
    def func(row):
        for k, v in row.items():
            if isinstance(v, str):
                row[k] = v.strip().replace('&nbsp;', ' ').replace('\xa0', ' ').replace('\r', '')
                if v == 'NULL':
                    row[k] = None
    return func

def keep_prominent():
    def func(rows):
        counts = dict()
        for row in rows:
            for k, v in row.items():
                counts.setdefault(k, dict(values=set(), total=0))
                if v not in (None, 'NULL', ''):
                    counts[k]['values'].add(str(v))
                    counts[k]['total'] += 1
            yield row
        counts = dict((k, (len(v['values']), v['total'], sorted(v['values'])[:1])) for k, v in counts.items() if len(v['values']) > 0)
        for x in sorted(counts.items(), key=lambda x: -x[1][1]):
            print(x)
    return func

def filter_results():

    def count(rows):
        i = 0
        found = set()
        for r in rows:
            t = r['type']
            if t not in found:
                print('???', t)
                found.add(t)
            for sf in SELECT_FIELDS:
                if sf not in ('page_url', AIRTABLE_ID_FIELD):
                    assert sf in r, '{} not in {}'.format(sf, r.keys())
            yield r
            i += 1
        print('###', i)

    return DF.Flow(
        count,
        DF.filter_rows(lambda row: row.get('lang_code') == 'he'),
        DF.set_type('type', type='integer', on_error=DF.schema_validator.drop),
        DF.filter_rows(lambda row: row.get('type') == 1),
        DF.filter_rows(lambda row: row.get('group_id') is not None),
        DF.filter_rows(lambda row: row.get('distribution_channel') is not None and row.get('distribution_channel')[0] == 1),
        count,
    )

def operator(*_):
    DF.Flow(
        load_from_airtable('appF3FyNsyk4zObNa', 'Click Lerevaha Mirror', 'Grid view'),
        DF.update_resource(-1, name='current'),
        scrape_click(),
        DF.update_resource(-1, name='click', path='data/click.csv'),
        DF.join('current', ['id'], 'click', ['id'], {
            AIRTABLE_ID_FIELD: None
        }),
        remove_nbsp(),
        filter_results(),
        # keep_prominent(),
        DF.add_field('page_url', 'string', lambda r: 'https://clickrevaha.molsa.gov.il/{Name}/product-page/{product_id}'.format(**r)),
        DF.select_fields(list(SELECT_FIELDS.keys())),
        DF.rename_fields(SELECT_FIELDS),
        DF.set_type('tags', type='array', transform=lambda v: v.split('|') if v else []),
        DF.set_type('delivery_channels', type='array', transform=lambda v: v.split('|') if v else []),
        DF.set_type('deductible', type='string', transform=lambda v: DEDUCTIBLE_TYPE.get(v)),
        DF.dump_to_path('click'),
        dump_to_airtable({
            ('appF3FyNsyk4zObNa', 'Click Lerevaha Mirror'): {
                'resource-name': 'click',
                'typecast': True
            }
        }),
    ).process()


if __name__ == '__main__':
    operator()