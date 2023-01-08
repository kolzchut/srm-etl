import logging
import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from dataflows_airtable import dump_to_airtable, load_from_airtable, AIRTABLE_ID_FIELD

from srm_tools.update_table import airtable_updater
from srm_tools.situations import Situations

from conf import settings
from srm_tools.logger import logger
from srm_tools.guidestar_api import GuidestarAPI


situations = Situations()


## SERVICES
def unwind_services(ga: GuidestarAPI, source='entities', existing_orgs = set()):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows        
        else:
            for _, row in enumerate(rows):
                if row['source'] != source:
                    continue
                regNum = row['id']

                existing_orgs.add(regNum)
                if len(existing_orgs) % 10 == 0:
                    print('COLLECTED {} organization services'.format(len(existing_orgs)))

                branches = ga.branches(regNum)
                if len(branches) == 0:
                    continue
                services = ga.services(regNum)
                for service in services:
                    if service.get('recordType') != 'GreenInfo':
                        continue
                    if not service.get('serviceName'):
                        continue
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
        DF.delete_fields(['source', 'status']),
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
        row['description'] = data.pop('voluntaryDescription') or data.pop('description')
        data_source_url = f'https://www.guidestar.org.il/organization/{data["organization_id"]}/services'
        row['data_sources'] = f'מידע נוסף אפשר למצוא ב<a href="{data_source_url}">גיידסטאר - אתר העמותות של ישראל</a>'
        orgId = data.pop('organization_id')
        row['organizations'] = [orgId]
        actual_branch_ids = data.pop('actual_branch_ids')
        row['branches'] = ['guidestar:' + b['branchId'] for b in (data.pop('branches') or []) if b['branchId'] in actual_branch_ids]
        if len(row['branches']) > 0:
            row['organizations'] = None

        record_type = data.pop('recordType')
        assert record_type == 'GreenInfo'
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

        details = []
        areas = []
        national = False

        area = (data.pop('area') or '').split(';')
        for item in area:
            if item == 'In Branches':
                areas.append('בסניפי הארגון')
            elif item == 'Country wide':
                areas.append('בתיאום מראש ברחבי הארץ')
                national = True
            elif item == 'Customer Place':
                areas.append('בבית הלקוח')
            elif item == 'Remote Service':
                areas.append('שירות מרחוק')
                national = True
            elif item == 'Via Phone or Mail':
                areas.append('במענה טלפוני, צ׳אט או בדוא"ל')
                national = True
            elif item == 'Web Service':
                areas.append('בשירות אינטרנטי מקוון')
                national = True
            elif item in ('Customer Appointment', 'Program', 'Not relevant', ''):
                pass
            else:
                assert False, 'area {}: {!r}'.format(area, row)

        if len(areas) > 1:
            details.append('השירות ניתן: ' + ', '.join(areas))
        elif len(areas) == 1:
            details.append('השירות ניתן ' + ''.join(areas))

        if national and row['organizations'] is not None:
            row['branches'] = [f'guidestar:{orgId}:national']
            row['organizations'] = None

        when = data.pop('whenServiceActive')
        if when == 'All Year':
            details.append('השירות ניתן בכל השנה')
        elif when == 'Requires Signup':
            details.append('השירות ניתן בהרשמה מראש')
        elif when == 'Time Limited':
            details.append('השירות מתקיים בתקופה מוגבלת')
        elif when == 'Criteria Based':
            details.append('השירות ניתן על פי תנאים או קריטריונים')
        elif when is None:
            pass
        else:
            assert False, 'when {}: {!r}'.format(when, row)

        remoteDelivery = (data.pop('remoteServiceDelivery') or '').split(';')
        # Phone, Chat / Email / Whatsapp, Internet, Zoom / Hybrid, Other
        methods = []
        for item in remoteDelivery:
            if item == 'Phone':
                methods.append('טלפון')
            elif item == 'Chat / Email / Whatsapp':
                methods.append('בצ׳אט, דוא"ל או וואטסאפ')
            elif item == 'Internet':
                methods.append('אתר אינטרנט')
            elif item == 'Zoom / Hybrid':
                methods.append('בשיחת זום')
            elif item == '':
                pass
            else:
                assert False, 'remoteDelivery {!r}: {!r}'.format(item, remoteDelivery)

        remoteDeliveryOther = data.pop('RemoteServiceDelivery_Other')
        if remoteDeliveryOther:
            methods.append(remoteDeliveryOther)

        if len(methods) > 0:
            details.append('שירות מרחוק באמצעות: ' + ', '.join(methods))
        

        row['details'] = '\n<br/>\n'.join(details)
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


def fetchServiceData(ga, taxonomy):
    print('FETCHING ALL ORGANIZATION SERVICES')
    existing_orgs = set()

    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'guidestar',
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'responses', 'organizations', 'branches', 'data_sources'],
        DF.Flow(
            load_from_airtable(settings.AIRTABLE_ENTITIES_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='orgs'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
            DF.select_fields(['id', 'name', 'source'], resources='orgs'),
            unwind_services(ga, existing_orgs=existing_orgs),
            # DF.checkpoint('unwind_services'),
        ),
        DF.Flow(
            updateServiceFromSourceData(taxonomy),
            # lambda rows: (r for r in rows if 'drop' in r), 
        ),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )

    return existing_orgs

### GUIDESTAR IMPORT FLOW

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
        row['urls'] = '\n'.join(urls)
        phone_numbers = []
        if data.get('tel1'):
            phone_numbers.append(data['tel1'])
        if data.get('tel2'):
            phone_numbers.append(data['tel2'])
        row['phone_numbers'] = '\n'.join(phone_numbers)
    return func

def fetchWildOrgData(ga: GuidestarAPI, skip_orgs):
    all_orgs = [
        org for org in ga.organizations() if org['id'] not in skip_orgs
    ]        
    print('COLLECTED {} relevant organizations'.format(len(all_orgs)))
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'guidestar',
        ['name', 'kind', 'urls', 'description', 'purpose', 'phone_numbers'],
        all_orgs,
        updateOrgFromSourceData(),
        airtable_base=settings.AIRTABLE_GUIDESTAR_IMPORT_BASE
    )
    return [org['id'] for org in all_orgs]

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
                    if branch.get('placeNickname'):
                        branch['name'] = branch['placeNickname']
                    else:
                        branch['name'] = (row['short_name'] or row['name']) + ' - ' + branch['cityName']
                    yield ret
                national = {}
                national.update(row)
                national['id'] = 'guidestar:' + regNum + ':national'
                national['data'] = {
                    'branchId': national['id'],
                    'name': row['name'],
                    'address': 'שירות ארצי',
                    'drivingInstructions': 'שירות ארצי',
                }
                yield national
    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
    )


def calc_location_key(row):
    if row.get('address'):
        return row['address']
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
        row['location'] = row['address']
        row['address_details'] = data.get('drivingInstructions')
        row['description'] = None
        row['urls'] = None
        # if data.get('branchURL'):
        #     row['urls'] = data['branchURL'] + '#הסניף בגיידסטאר'
        row['phone_numbers'] = None
        if data.get('phone'):
            row['phone_numbers'] = data['phone']
        if row.get('organization_id'):
            row['organization'] = [row['organization_id']]
        if data.get('language'):
            row['situations'] = [
                'human_situations:language:{}_speaking'.format(l.lower().strip()) for l in data['language'].split(';')
            ]
    return func


def fetchWildBranchData(ga):
    print('FETCHING ALL ORGANIZATION BRANCHES')
    airtable_updater(settings.AIRTABLE_BRANCH_TABLE, 'guidestar',
        ['name', 'organization', 'address', 'address_details', 'location', 'description', 'phone_numbers', 'urls', 'situations'],
        DF.Flow(
            load_from_airtable(settings.AIRTABLE_GUIDESTAR_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW),
            DF.update_resource(-1, name='orgs'),
            DF.filter_rows(lambda r: r['source'] == 'guidestar', resources='orgs'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
            DF.rename_fields({
                AIRTABLE_ID_FIELD: 'organization_id',
            }, resources='orgs'),
            DF.select_fields(['organization_id', 'id', 'name', 'short_name'], resources='orgs'),
            unwind_branches(ga),
        ),
        updateBranchFromSourceData(),
        airtable_base=settings.AIRTABLE_GUIDESTAR_IMPORT_BASE
    )

## Services

def fetchWildServiceData(ga, taxonomy):
    print('FETCHING ALL ORGANIZATION SERVICES')
    existing_orgs = set()

    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'guidestar',
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'responses', 'organizations', 'branches', 'data_sources'],
        DF.Flow(
            load_from_airtable(settings.AIRTABLE_GUIDESTAR_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
            DF.update_resource(-1, name='orgs'),
            DF.filter_rows(lambda r: r['status'] == 'ACTIVE', resources='orgs'),
            DF.select_fields(['id', 'name', 'source'], resources='orgs'),
            unwind_services(ga, source='guidestar'),
            # DF.checkpoint('unwind_services'),
        ),
        updateServiceFromSourceData(taxonomy),
        airtable_base=settings.AIRTABLE_GUIDESTAR_IMPORT_BASE
    )

    return existing_orgs


def operator(name, params, pipeline):
    logger.info('STARTING Guidestar Scraping')
    ga = GuidestarAPI()

    print('FETCHING TAXONOMY MAPPING')
    taxonomy = DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_GUIDESTAR_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        # DF.printer(),
        # DF.select_fields(['name', 'situation_ids', 'response_ids']),
    ).results()[0][0]
    taxonomy = dict(
        (r.pop('name'), r) for r in taxonomy
    )

    skip_orgs = fetchServiceData(ga, taxonomy)

    logger.info('FETCHING Org data for wild organizations')
    fetchWildOrgData(ga, skip_orgs)
    fetchWildBranchData(ga)
    fetchWildServiceData(ga, taxonomy)


if __name__ == '__main__':
    import logging
    import sys
    logger.setLevel(logging.DEBUG)
    logger.info('STARTING Guidestar Scraping')
    logger.addHandler(logging.StreamHandler(sys.stdout))
    operator(None, None, None)