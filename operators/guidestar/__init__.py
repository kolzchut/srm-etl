from srm_tools.budgetkey import fetch_from_budgetkey
import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

try:
    from .guidestar_api import GuidestarAPI
except ImportError:
    from guidestar_api import GuidestarAPI

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from srm_tools.logger import logger
from srm_tools.update_table import airflow_table_update_flow, airflow_table_updater
from srm_tools.situations import Situations

from conf import settings


situations = Situations()


## ORGANIZATIONS
def updateOrgFromSourceData():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        row['name'] = data['name'].replace(' (חל"צ)', '').replace(' (ע"ר)', '')
        row['kind'] = data['malkarType']
        row['description'] = None
        row['purpose'] = data.get('orgGoal')
        urls = []
        if data.get('website'):
            urls.append(data['website'] + '#אתר הבית')
        if data.get('urlGuidestar'):
            urls.append(data['urlGuidestar'] + '#הארגון בגיידסטאר')
        row['urls'] = '\n'.join(urls)
    return func

def fetchOrgData(ga):
    print('FETCHING ALL ORGANIZATIONS')
    query = '''
        with suppliers as (select jsonb_array_elements(suppliers) as supplier from activities where suppliers is not null and suppliers::text != 'null' and catalog_number is not null)
        select supplier->>'entity_id' as entity_id, supplier->>'entity_kind' as entity_kind from suppliers
    '''
    social_service_entity_ids = fetch_from_budgetkey(query)
    regNums = sorted(set(
        row['entity_id'] for row in social_service_entity_ids
        if row['entity_kind'] == 'association'
    ))
    print('COLLECTED {} relevant organizations'.format(len(regNums)))
    airflow_table_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'guidestar',
        ['name', 'kind', 'urls', 'description', 'purpose'],
        ga.organizations(regNums=regNums),
        updateOrgFromSourceData()
    )


## BRANCHES
def unwind_branches(ga:GuidestarAPI):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows        
        else:
            for _, row in enumerate(rows):
                regNum = row['id']
                branches = ga.branches(regNum)
                for branch in branches:
                    ret = dict()
                    ret.update(row)
                    ret['data'] = branch
                    ret['id'] = 'guidestar:' + branch['branchId']
                    branch['name'] = row['name'] + ' - ' + branch['cityName']
                    yield ret
    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
    )

def calc_location_key(row):
    key = ''
    cityName = row.get('cityName')
    if cityName:
        streetName = row.get('streetName')
        if streetName:
            key += f'{streetName} '
            houseNum = row.get('houseNum')
            if houseNum:
                key += f'{houseNum} '
            key += ', '
        key += f'{cityName} '
    
    alternateAddress = row.get('alternateAddress')
    if alternateAddress:
        if alternateAddress not in key:
            key += f' - {alternateAddress}'
    key = key.strip()

    return key or None

def updateBranchFromSourceData():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        # print('data', data)
        # print('row', row)
        row['name'] = data['name']
        row['address'] = calc_location_key(data)
        row['location'] = [row['address']]
        row['address_details'] = data.get('drivingInstructions')
        row['description'] = None
        row['urls'] = None
        if data.get('branchURL'):
            row['urls'] = data['branchURL'] + '#הסניף בגיידסטאר'
        row['phone_numbers'] = None
        if data.get('phone'):
            row['phone_numbers'] = data['phone']
        if row.get('organization_id'):
            row['organization'] = [row['organization_id']]
        if data.get('language'):
            row['situations'] = situations.convert_situation_list([
                'human_situations:language:{}_speaking'.format(l.lower().strip()) for l in data['language'].split(';')
            ])

    return func


def fetchBranchData(ga):
    print('FETCHING ALL ORGANIZATION BRANCHES')
    DF.Flow(
        airflow_table_update_flow(settings.AIRTABLE_BRANCH_TABLE, 'guidestar',
            ['name', 'organization', 'address', 'address_details', 'location', 'description', 'phone_numbers', 'urls', 'situations'],
            DF.Flow(
                load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW),
                DF.update_resource(-1, name='orgs'),
                DF.filter_rows(lambda r: r['source'] == 'guidestar', resources='orgs'),
                DF.rename_fields({
                    AIRTABLE_ID_FIELD: 'organization_id',
                }, resources='orgs'),
                DF.select_fields(['organization_id', 'id', 'name'], resources='orgs'),
                unwind_branches(ga),
            ),
            DF.Flow(
                updateBranchFromSourceData(),
            )
        )
    ).process()

## SERVICES
def unwind_services(ga: GuidestarAPI):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows        
        else:
            for _, row in enumerate(rows):
                regNum = row['id']
                services = ga.services(regNum)
                for service in services:
                    ret = dict()
                    ret.update(row)
                    ret['data'] = service
                    ret['data']['organization_id'] = row['id']
                    ret['id'] = 'guidestar:' + service['serviceId']
                    yield ret
    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
    )

def updateServiceFromSourceData(taxonomies):
    def update_from_taxonomy(names, responses, situations):
        for name in names:
            if name:
                mapping = taxonomies[name]
                responses.update(mapping['response_ids'] or [])
                situations.update(mapping['situation_ids'] or [])

    def func(row):
        data = row['data']

        responses = set()
        situations = set()

        row['name'] = data.pop('serviceName')
        row['description'] = data.pop('description')
        row['organizations'] = [data.pop('organization_id')]
        row['branches'] = ['guidestar:' + b['branchId'] for b in (data.pop('branches') or [])]
        if data.pop('isForBranch'):
            assert len(row['branches']) > 0, repr(row)
            row['organizations'] = None

        record_type = data.pop('recordType')
        assert record_type in ('GreenInfo', 'YouthProject'), record_type
        if record_type == 'GreenInfo':
            for k in list(data.keys()):
                if k.startswith('youth'):
                    data.pop(k)
            update_from_taxonomy([data.pop('serviceTypeName')], responses, situations)
            update_from_taxonomy((data.pop('serviceTargetAudience') or '').split(';'), responses, situations)

            payment_required = data.pop('paymentMethod')
            if payment_required == 'Free service':
                row['payment_required'] = 'no'
            elif payment_required == 'Symbolic cost':
                row['payment_required'] = 'yes'
                row['payment_details'] = 'עלות סמלית'
            elif payment_required == 'Full payment':
                row['payment_required'] = 'yes'
                row['payment_details'] = 'השירות ניתן בתשלום'
            elif payment_required == 'Government funded':
                row['payment_required'] = 'yes'
                row['payment_details'] = 'השירות מסובסד על ידי הממשלה'
            else:
                assert False, payment_required + ' ' + repr(row)

            area = data.pop('area')
            if area == 'All branches':
                row['details'] = 'השירות ניתן בסניפי הארגון'
            elif area == 'Some branches':
                row['details'] = 'השירות ניתן בחלק מהסניפים של הארגון'
            elif area == 'Program':
                row['details'] = 'תוכנית ייעודית בהרשמה מראש'
            elif area == 'Customer Appointment':
                row['details'] = 'בתיאום מראש ברחבי הארץ'
            elif area == 'Country wide':
                row['details'] = 'במפגשים קבוצתיים או אישיים'
            elif area == 'Web Service':
                row['details'] = 'שירות אינטרנטי מקוון'
            elif area == 'Via Phone or Mail':
                row['details'] = 'במענה טלפוני או בדוא"ל'
            elif area == 'Not relevant':
                pass
            else:
                assert False, area + ' ' + repr(row)
            
        elif record_type == 'YouthProject':
            assert data.pop('serviceTypeName') == 'תוכניות לצעירים'
            details = ''
            main_topic = data.pop('projectTopic_Main')
            if main_topic == 'אחר':
                main_topic = None
            else:
                update_from_taxonomy([main_topic], responses, situations)
            other = data.pop('projectTopicMainOther')
            if other:
                details += other + '\n'

            secondary_topics = (data.pop('projectTopic_Secondary') or '').split(';')
            if 'אחר' in secondary_topics:
                secondary_topics.remove('אחר')
            other = data.pop('projectTopicSecondary_Other')
            if other:
                details += 'נושאים נוספים:' + other + '\n'
            update_from_taxonomy(secondary_topics, responses, situations)

            target_audience = data.pop('youthTargetAudience').split(';')
            if 'אחר' in target_audience:
                target_audience.remove('אחר')
            update_from_taxonomy(target_audience, responses, situations)

            other = data.pop('youthTargetAudienceOther')
            if other:
                details += 'קהל יעד:' + other + '\n'

            intervention_type = data.pop('youthActivityInterventionType').split(';')
            if 'אחר' in intervention_type:
                intervention_type.remove('אחר')
            other = data.pop('youthActivityInterventionTypeOther')
            if other:
                details += 'אופן מתן השירות:' + other + '\n'
            update_from_taxonomy(intervention_type, responses, situations)

            target_age = data.pop('targetAge').split(';')
            update_from_taxonomy(target_age, responses, situations)

        url = data.pop('url')
        if url and url.startswith('http'):
            row['url'] = f'{url}#מידע נוסף על השירות'

        for k in ('isForCoronaVirus', 'lastModifiedDate', 'serviceId', 'regNum'):
            data.pop(k)
        row['situations'] = sorted(situations)
        row['responses'] = sorted(responses)
        assert all(v in (None, '0') for v in data.values()), repr(row)
    return DF.Flow(
        func,
    )


def fetchServiceData(ga):
    print('FETCHING ALL ORGANIZATION SERVICES')
    print('FETCHING TAXONOMY MAPPING')
    taxonomy = DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, 'Guidestar Service Taxonomy Mapping', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        # DF.printer(),
        # DF.select_fields(['name', 'situation_ids', 'response_ids']),
    ).results()[0][0]
    taxonomy = dict(
        (r.pop('name'), r) for r in taxonomy
    )

    airflow_table_updater(settings.AIRTABLE_SERVICE_TABLE, 'guidestar',
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'responses', 'organizations', 'branches'],
        DF.Flow(
            load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='orgs'),
            DF.filter_rows(lambda r: r['source'] == 'guidestar', resources='orgs'),
            DF.rename_fields({
                AIRTABLE_ID_FIELD: 'organization_id',
            }, resources='orgs'),
            DF.select_fields(['organization_id', 'id', 'name'], resources='orgs'),
            unwind_services(ga),
            # DF.checkpoint('unwind_services'),
        ),
        DF.Flow(
            updateServiceFromSourceData(taxonomy),
            # lambda rows: (r for r in rows if 'drop' in r), 
        )
    )


def operator(name, params, pipeline):
    logger.info('STARTING Guidestar Scraping')
    ga = GuidestarAPI()

    fetchOrgData(ga)
    fetchBranchData(ga)
    fetchServiceData(ga)


if __name__ == '__main__':
    operator(None, None, None)
