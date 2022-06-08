import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from dataflows_airtable import dump_to_airtable, load_from_airtable

from srm_tools.update_table import airtable_updater
from srm_tools.situations import Situations

from conf import settings
from srm_tools.logger import logger
from srm_tools.guidestar_api import GuidestarAPI


situations = Situations()


## SERVICES
def unwind_services(ga: GuidestarAPI):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows        
        else:
            for _, row in enumerate(rows):
                regNum = row['id']
                branches = ga.branches(regNum)
                if len(branches) == 0:
                    continue
                services = ga.services(regNum)
                for service in services:
                    ret = dict()
                    ret.update(row)
                    ret['data'] = service
                    ret['data']['organization_id'] = regNum
                    ret['data']['actual_branch_ids'] = [b['branchId'] for b in branches]
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
                try:
                    mapping = taxonomies[name]
                    responses.update(mapping['response_ids'] or [])
                    situations.update(mapping['situation_ids'] or [])
                except KeyError:
                    print('WARNING: no mapping for {}'.format(name))
                    DF.Flow(
                        [dict(name=name)],
                        DF.update_resource(-1, name='taxonomies'),
                        dump_to_airtable({
                            (settings.AIRTABLE_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_GUIDESTAR_TABLE): {
                                'resource-name': 'taxonomies',
                                'typecast': True
                            }
                        }, settings.AIRTABLE_API_KEY),
                    ).process()

    def func(row):
        if 'data' not in row:
            # print('NO DATA', row)
            return
        data = row['data']

        responses = set()
        situations = set()

        row['name'] = data.pop('serviceName')
        row['description'] = data.pop('description')
        row['organizations'] = [data.pop('organization_id')]
        actual_branch_ids = data.pop('actual_branch_ids')
        row['branches'] = ['guidestar:' + b['branchId'] for b in (data.pop('branches') or []) if b['branchId'] in actual_branch_ids]
        if len(row['branches']) > 0:
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
            elif area == 'Customer Place':
                row['details'] = 'אצל מקבלי השירות'
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

            target_audience = (data.pop('youthTargetAudience') or '').split(';')
            if 'אחר' in target_audience:
                target_audience.remove('אחר')
            update_from_taxonomy(target_audience, responses, situations)

            other = data.pop('youthTargetAudienceOther')
            if other:
                details += 'קהל יעד:' + other + '\n'

            data.pop('youthActivity_Area', None)

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
            row['urls'] = f'{url}#מידע נוסף על השירות'

        for k in ('isForCoronaVirus', 'lastModifiedDate', 'serviceId', 'regNum', 'isForBranch'):
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
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_GUIDESTAR_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        # DF.printer(),
        # DF.select_fields(['name', 'situation_ids', 'response_ids']),
    ).results()[0][0]
    taxonomy = dict(
        (r.pop('name'), r) for r in taxonomy
    )

    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'guidestar',
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'responses', 'organizations', 'branches'],
        DF.Flow(
            load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='orgs'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
            DF.select_fields(['id', 'name'], resources='orgs'),
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

    fetchServiceData(ga)


if __name__ == '__main__':
    operator(None, None, None)
