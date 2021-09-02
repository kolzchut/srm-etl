import json
from srm_tools.budgetkey import fetch_from_budgetkey
from dataflows.base.flow import Flow
from datapackage import resource

import requests

import dataflows as DF

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from srm_tools.logger import logger
from srm_tools.update_table import airflow_table_updater
from srm_tools.situations import Situations

from conf import settings

KEEP_FIELDS = ['cat', 'Name']
DT_SUFFIXES = dict((k, i) for i, k in enumerate(['', 'i', 'ss', 't', 's', 'base64', 'f', 'is']))
SELECT_FIELDS = {
    'id': 'catalog_number',
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
}
DEDUCTIBLE_TYPE = {
    'אינו כרוך בהשתתפות עצמית': 'no',
    'בחלק מהמקרים תתכן השתתפות עצמית': 'sometimes',
    'כרוך בהשתתפות עצמית': 'yes'
}
situations = Situations()


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


def fetch_organizations():
    def func(rows):
        results = fetch_from_budgetkey('''
            select catalog_number, jsonb_array_elements(suppliers) as supplier from activities
                where suppliers is not null and suppliers::text != 'null'
        ''')
        results = list(results)
        print('GOT {} SUPPLIERS'.format(len(results)))
        suppliers = dict()
        for rec in results:
            suppliers.setdefault(rec['catalog_number'], []).append(rec['supplier']['entity_id'])

        for row in rows:
            catalog_number = row.get('catalog_number')
            row['organizations'] = sorted(set(suppliers.get(catalog_number, [])), reverse=True)
            yield row
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
    
    records = DF.Flow(
        docs,
        DF.concatenate(concat_fields),
        DF.update_resource(-1, name='click'),
        remove_nbsp(),
        filter_results(),
        DF.add_field('page_url', 'string', lambda r: 'https://clickrevaha.molsa.gov.il/{Name}/product-page/{product_id}'.format(**r)),
        DF.select_fields(list(SELECT_FIELDS.keys())),
        DF.rename_fields(SELECT_FIELDS),
        DF.add_field('organizations', 'array', []),
        fetch_organizations(),
        DF.set_type('tags', type='array', transform=lambda v: v.split('|') if v else []),
        DF.set_type('delivery_channels', type='array', transform=lambda v: v.split('|') if v else []),
        DF.set_type('deductible', type='string', transform=lambda v: DEDUCTIBLE_TYPE.get(v)),
    ).results()[0][0]
    return [
        dict(id='clr-{}'.format(r['catalog_number']), data=r)
        for r in records
    ]

def updateServiceFromSourceData():
    def func(row):
        data = row.get('data')
        if not data:
            return
        row['name'] = data['title']
        row['description'] = data['subtitle']
        row['details'] = '\n\n'.join(data[f].strip() for f in [
            'description', 'details', 'implementation_details','target_community_text', 'service_duration_text'
        ] if data.get(f))
        row['payment_required'] = data['deductible']
        row['payment_details'] = data['deductible_details']
        row['urls'] = data['page_url'] + '#דף השירות בקליק לרווחה'
        row['organizations'] = data['organizations']
        row['situations'] = situations.situations_for_age_range(data['age_min'], data['age_max'])
        row['situations'].extend(situations.situations_for_clr_target_population(data.get('target_populations_level_1') or []))
        row['situations'].extend(situations.situations_for_clr_target_population(data.get('target_populations_level_2') or []))
        row['situations'] = situations.convert_situation_list(sorted(set(row['situations'])))
    return func

def operator(*_):
    airflow_table_updater(
        'Services', 'click-lerevaha',
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'organizations'],
        scrape_click(),
        updateServiceFromSourceData()
    )


if __name__ == '__main__':
    operator()
