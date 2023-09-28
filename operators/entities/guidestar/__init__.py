import logging
import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from dataflows_airtable import dump_to_airtable, load_from_airtable, AIRTABLE_ID_FIELD

from openlocationcode import openlocationcode as olc

from srm_tools.update_table import airtable_updater
from srm_tools.situations import Situations

from conf import settings
from srm_tools.logger import logger
from srm_tools.guidestar_api import GuidestarAPI
from srm_tools.url_utils import fix_url

situations = Situations()


## SERVICES
def unwind_services(ga: GuidestarAPI, source='entities', existing_orgs = set()):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows        
        else:
            count = 0
            for _, row in enumerate(rows):
                if row['source'] != source:
                    continue
                regNum = row['id']

                existing_orgs.add(regNum)

                branches = ga.branches(regNum)
                # if len(branches) == 0:
                #     continue
                services = ga.services(regNum)
                govServices = dict(
                    (s['relatedMalkarService'], s) for s in services if s.get('serviceGovName') is not None and s.get('relatedMalkarService') is not None
                )
                for service in services:
                    if service['serviceId'] in govServices:
                        print('GOT RELATED SERVICE', service['serviceId'])
                        service['relatedMalkarService'] = govServices.get(service['serviceId'])
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
                    count += 1
                    if count % 10 == 0:
                        print('COLLECTED {} organization\'s {} services'.format(len(existing_orgs), count))
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
                    taxonomies[name] = dict(response_ids=[], situation_ids=[])
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

    def func(rows):
        for row in rows:
            if 'data' not in row:
                # print('NO DATA', row)
                yield row
                continue

            data = row['data']

            responses = set()
            situations = set()

            row['name'] = data.pop('serviceName')
            row['description'] = data.pop('voluntaryDescription') or data.pop('description')
            data_source_url = f'https://www.guidestar.org.il/organization/{data["organization_id"]}/services'
            row['data_sources'] = f'מידע נוסף אפשר למצוא ב<a target="_blank" href="{data_source_url}">גיידסטאר - אתר העמותות של ישראל</a>'
            orgId = data.pop('organization_id')
            actual_branch_ids = data.pop('actual_branch_ids')
            row['branches'] = ['guidestar:' + b['branchId'] for b in (data.pop('branches') or []) if b['branchId'] in actual_branch_ids]

            record_type = data.pop('recordType')
            assert record_type == 'GreenInfo'
            for k in list(data.keys()):
                if k.startswith('youth'):
                    data.pop(k)

            relatedMalkarService = data.pop('relatedMalkarService') or {}

            update_from_taxonomy([data.pop('serviceTypeName')], responses, situations)
            update_from_taxonomy((data.pop('serviceTargetAudience') or '').split(';'), responses, situations)
            update_from_taxonomy(['soproc:' + relatedMalkarService.get('serviceGovId', '')], responses, situations)

            payment_required = data.pop('paymentMethod')
            if payment_required in ('Free service', None):
                row['payment_required'] = 'no'
                row['payment_details'] = None
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

            service_terms = data.pop('serviceTerms')
            if service_terms:
                if row.get('payment_details'):
                    row['payment_details'] += ', ' + service_terms
                else:
                    row['payment_details'] = service_terms

            details = []
            areas = []
            national = False

            area = (data.pop('area') or '').split(';')
            for item in area:
                if item == 'In Branches':
                    areas.append('בסניפי הארגון')
                    if len(row['branches']) == 0:
                        row['branches'] = ['guidestar:' + bid for bid in actual_branch_ids]
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
                elif item == 'Customer Appointment':
                    areas.append('במפגשים קבוצתיים או אישיים')
                elif item == 'Program':
                    areas.append('תוכנית ייעודית בהרשמה מראש')
                elif item in ('Not relevant', ''):
                    pass
                else:
                    assert False, 'area {}: {!r}'.format(area, row)

            if len(areas) > 1:
                details.append('השירות ניתן: ' + ', '.join(areas))
            elif len(areas) == 1:
                details.append('השירות ניתן ' + ''.join(areas))

            if national:
                row['branches'].append(f'guidestar:{orgId}:national')
            if len(row['branches']) == 0:
                continue

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
                elif item == 'Other':
                    pass
                else:
                    assert False, 'remoteDelivery {!r}: {!r}'.format(item, remoteDelivery)

            remoteDeliveryOther = data.pop('RemoteServiceDelivery_Other')
            if remoteDeliveryOther:
                methods.append(remoteDeliveryOther)

            if len(methods) > 0:
                details.append('שירות מרחוק באמצעות: ' + ', '.join(methods))

            if relatedMalkarService:
                relatedId = relatedMalkarService.get('serviceGovId')
                relatedOffice = relatedMalkarService.get('serviceOffice')
                print('GOT RELATED: id={}, office={}'.format(relatedId, relatedOffice))
                if relatedId and relatedOffice:
                    row['implements'] = f'soproc:{relatedId}#{relatedOffice}'

            row['details'] = '\n<br/>\n'.join(details)
            url = data.pop('url')
            url = fix_url(url)
            if url:
                row['urls'] = f'{url}#מידע נוסף על השירות'

            phone_numbers = data.pop('Phone', data.pop('phone', None))
            if phone_numbers:
                row['phone_numbers'] = phone_numbers

            email_address = data.pop('Email', data.pop('email', None))
            if email_address:
                row['email_address'] = email_address

            for k in ('isForCoronaVirus', 'lastModifiedDate', 'serviceId', 'regNum', 'isForBranch'):
                data.pop(k)
            row['situations'] = sorted(situations)
            row['responses'] = sorted(responses)
            assert all(v in (None, '0') for v in data.values()), repr(data_source_url) + ':' + repr(data)
            yield row

    return DF.Flow(
        func,
    )


def fetchServiceData(ga, taxonomy):
    print('FETCHING ALL ORGANIZATION SERVICES')
    existing_orgs = set()

    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'guidestar',
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'responses', 
        'organizations', 'branches', 'data_sources', 'implements', 'phone_numbers', 'email_address'],
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
        if data.get('abbreviatedOrgName'):
            row['short_name'] = data['abbreviatedOrgName']
        row['kind'] = data['malkarType']
        row['description'] = None
        row['purpose'] = data.get('orgGoal')
        urls = []
        if data.get('website'):
            website = fix_url(data['website'])
            if website:
                urls.append(website + '#אתר הבית')
        row['urls'] = '\n'.join(urls)
        phone_numbers = []
        if data.get('tel1'):
            phone_numbers.append(data['tel1'])
        if data.get('tel2'):
            phone_numbers.append(data['tel2'])
        row['phone_numbers'] = '\n'.join(phone_numbers)
        if data.get('email'):
            row['email_address'] = data['email']
    return func

def fetchWildOrgData(ga: GuidestarAPI, skip_orgs):
    all_orgs = [
        org for org in ga.organizations() if org['id'] not in skip_orgs
    ]        
    print('COLLECTED {} relevant organizations'.format(len(all_orgs)))
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'guidestar',
        ['name', 'short_name', 'kind', 'urls', 'description', 'purpose', 'phone_numbers', 'email_address'],
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
                        branch['name'] = (row.get('short_name') or row.get('name')) + ' - ' + branch['cityName']
                    yield ret
                national = {}
                national.update(row)
                national['id'] = 'guidestar:' + regNum + ':national'
                national['data'] = {
                    'branchId': national['id'],
                    'organization_id': regNum,
                    'name': row['name'],
                    'address': 'שירות ארצי',
                    'location': 'שירות ארצי',
                }
                yield national
    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
    )


def calc_address(row):
    if row.get('address'):
        return row['address']
    key = ''
    cityName = row.get('cityName')
    if cityName:
        cityName = cityName.replace(' תאי דואר', '')
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

def calc_location_key(src, dst):
    y, x = src.get('latitude'), src.get('longitude')
    if y and x:
        code = olc.encode(y, x, 11)
    else:
        code = None
    return code or dst['address']

def updateBranchFromSourceData():
    def func(row):
        data = row.get('data')
        if data is None:
            return
        # print('data', data)
        # print('row', row)
        row['name'] = data['name']
        row['address'] = calc_address(data)
        row['location'] = calc_location_key(data, row)
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
            load_from_airtable(settings.AIRTABLE_GUIDESTAR_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
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
        ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations', 'responses',
         'organizations', 'branches', 'data_sources', 'implements', 'phone_numbers', 'email_address'],
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
        DF.select_fields(['name', 'situation_ids', 'response_ids']),
    ).results()[0][0]
    taxonomy = dict(
        (r.pop('name'), r) for r in taxonomy
    )

    print('FETCHING SOPROC MAPPING')
    soproc_mappings = DF.Flow(
        load_from_airtable(settings.AIRTABLE_ENTITIES_IMPORT_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_SOPROC_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.select_fields(['id', 'situation_ids', 'response_ids']),
    ).results()[0][0]
    taxonomy.update(dict(
        (r.pop('id'), r) for r in soproc_mappings
    ))

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
    # operator(None, None, None)
    ga = GuidestarAPI()
    taxonomy = dict()
    orgs = [
        dict(source='entities', id='580019800'),
    ]
    DF.Flow(
        orgs,
        DF.update_resource(-1, name='orgs'),
        unwind_services(ga),
        updateServiceFromSourceData(taxonomy),
        DF.printer(),
    ).process()
