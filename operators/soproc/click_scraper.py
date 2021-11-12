import json
import requests

import dataflows as DF
from dataflows_airtable import load_from_airtable

from conf import settings

KEEP_FIELDS = ['cat', 'Name']
DT_SUFFIXES = dict((k, i) for i, k in enumerate(['', 'i', 'ss', 't', 's', 'base64', 'f', 'is']))
SELECT_FIELDS = {
    'id': 'catalog_number',
    'urls': 'urls',

    # 'DisplayName': '',

    'Administration': 'department',
    'parent_group_name': 'service_group',
    'group_name': 'unit',
    'FamilyName': 'name',

    'Service_Purpose': 'purpose',
    'Short_Description': 'description',
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

    'Deducitable': 'payment_required',
    'Deductible': 'payment_details',
    # 'Relationship_Type', # TODO: See if useful
    
    'Service_Status': 'service_status',
    'Service_Channels': 'delivery_channels',
    # 'Right_or_Service', # TODO: See if useful
    'Program_Activation_Model': 'internal_operation_model',
    # 'location_type', # TODO: Probably not useful
    
    'Implementaion_Process': 'implementation_details',
    
    'Link_to_Kolzchut': 'link_to_kolzchut',
}
DEDUCTIBLE_TYPE = {
    'אינו כרוך בהשתתפות עצמית': 'no',
    'בחלק מהמקרים תתכן השתתפות עצמית': 'sometimes',
    'כרוך בהשתתפות עצמית': 'yes'
}


def remove_nbsp():
    def func(row):
        for k, v in row.items():
            if isinstance(v, str):
                row[k] = v.strip().replace('&nbsp;', ' ').replace('\xa0', ' ').replace('\r', '')
                if v == 'NULL':
                    row[k] = None
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
        docs = requests.get(settings.CLICK_API).json().get('response').get('docs')
        json.dump(docs, open('click-cache.json', 'w'))

    # print(len(docs))
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
        prefix, k, _ = sorted(suffixes, key=lambda s: DT_SUFFIXES[s[2]])[0]
        # prefix = prefix.lower()
        concat_fields[prefix] = [k] if prefix != k else []

    docs = (
        dict((k, doc.get(k)) for k in all_keys)
        for doc in docs
    )
    # print(next(docs))
    
    taxonomy = DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, 'Click Service Taxonomy Mapping', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    ).results()[0][0]
    taxonomy = dict(
        (r.pop('name'), r) for r in taxonomy
    )

    records = DF.Flow(
        docs,
        DF.concatenate(concat_fields),
        DF.update_resource(-1, name='click'),
        remove_nbsp(),
        filter_results(),
        DF.add_field('urls', 'string', lambda r: 'https://clickrevaha.molsa.gov.il/{Name}/product-page/{product_id}#השירות בהרחבה ב״קליק לרווחה״'.format(**r)),
        DF.select_fields(list(SELECT_FIELDS.keys())),
        DF.rename_fields(SELECT_FIELDS),
        DF.set_type('details',
            transform=lambda _, row: '\n\n'.join(row[f].strip() for f in [
                'description', 'details', 'implementation_details','target_community_text', 'service_duration_text'
            ] if row.get(f))
        ),
        DF.set_type('tags', type='array', 
            transform=lambda v, row: (
                (v.split('|') if v else []) +
                ['age-{age_min}-{age_max}'.format(**row)] +
                (row.get('target_populations_level_1') or []) +
                (row.get('target_populations_level_2') or []) +
                (row.get('service_subject') or [])
            )
        ),
        DF.set_type('delivery_channels', type='array', transform=lambda v: v.split('|') if v else []),
        DF.set_type('payment_required', type='string', transform=lambda v: DEDUCTIBLE_TYPE.get(v)),
        DF.add_field('situations', 'array', fetch_from_taxonomy(taxonomy, 'situation_ids')),
        DF.add_field('responses', 'array', fetch_from_taxonomy(taxonomy, 'response_ids')),
        DF.select_fields(['catalog_number', 'name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'responses']),
    ).results()[0][0]
    return dict((r['catalog_number'], r) for r in records)


if __name__ == '__main__':
    sc = scrape_click()
    print(len(sc))
    import pprint
    pprint.pprint(sc['143'])
    # all_tags = [t for v in sc.values() for t in v['tags']]
    # with open('tags', 'w') as t:
    #     t.write('\n'.join(sorted(set(all_tags))))