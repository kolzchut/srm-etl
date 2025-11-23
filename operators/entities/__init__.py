import datetime
import dateutil.parser

import dataflows as DF
from dataflows.base.resource_wrapper import ResourceWrapper

from dataflows_airtable import dump_to_airtable, load_from_airtable
from dataflows_airtable.consts import AIRTABLE_ID_FIELD

from openlocationcode import openlocationcode as olc

from srm_tools.stats import Report, Stats
from srm_tools.update_table import airtable_updater
from srm_tools.guidestar_api import GuidestarAPI
from srm_tools.budgetkey import fetch_from_budgetkey
from srm_tools.data_cleaning import clean_org_name
from srm_tools.processors import update_mapper

from conf import settings
from srm_tools.logger import logger
from srm_tools.url_utils import fix_url
from srm_tools.error_notifier import invoke_on

isDebug = False


## ORGANIZATIONS
def fetchEntityFromBudgetKey(regNum):
    entity = list(fetch_from_budgetkey(f"select * from entities where id='{regNum}'"))
    if len(entity) > 0:
        entity = entity[0]
        name = entity['name']
        purpose = entity['details'].get('goal')
        if regNum.startswith('50'):
            purpose = purpose or name
            name = name.split('/')[0].strip()
        rec = dict(
            id=entity['id'],
            data=dict(
                name=name,
                kind=entity['kind_he'],
                purpose=purpose
            )
        )
        return rec


def updateOrgFromSourceData(ga: GuidestarAPI, stats: Stats):
    unknown_entity_ids = Report(
        'Entities: Unknown ID Report',
        'entities-unknown-id-report',
        ['id', 'kind'],
        ['id']
    )

    def func(rows):
        for row in rows:
            regNums = [row['id']]
            if row['id'].startswith('srm'):
                yield row
                continue
            # if row['kind'] is not None:
            #     continue
            for data in ga.organizations(regNums=regNums, cacheOnly=True):
                try:
                    data = data['data']
                    row['name'] = data['name']
                    row['short_name'] = data.get('abbreviatedOrgName')
                    kind = data.get('malkarType')
                    if kind == "חברה":
                        kind = "חברה פרטית"
                    row['kind'] = kind
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
                    break
                except Exception as e:
                    print('BAD DATA RECEIVED', str(e), regNums, data)
            else:
                data = fetchEntityFromBudgetKey(row['id'])
                if data is not None:
                    row.update(data['data'])
                else:
                    stats.increase('Entities: Unknown ID')
                    unknown_entity_ids.add(row)
            if 'name' in row:
                row['name'] = row['name'].replace(' (חל"צ)', '').replace(' (ע"ר)', '')
            yield row
        unknown_entity_ids.save()

    return func


def recent_org(row):
    ltd = row.get('last_tag_date')
    if ltd is not None:
        ltd = dateutil.parser.isoparse(ltd)
        days = (ltd.now() - ltd).days
        if days < 15:
            return True
    return False


def fetchOrgData(ga, stats: Stats):
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE,
                           settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='orgs'),
        DF.filter_rows(lambda row: row.get('source') == 'entities'),
        DF.select_fields([AIRTABLE_ID_FIELD, 'id', 'kind']),
        updateOrgFromSourceData(ga, stats),
        dump_to_airtable({
            (settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE): {
                'resource-name': 'orgs',
            }
        }, settings.AIRTABLE_API_KEY)
    ).process()

## EXTERNAL DEDUPLICATION FUNCTION
def deduplicate_items(items):
    """
    Takes a list of dicts with 'id' field, returns only unique items based on 'id'.
    Keeps the first occurrence of each ID.
    """
    seen = set()
    deduped = []
    for item in items:
        item_id = item['id']
        if item_id not in seen:
            seen.add(item_id)
            deduped.append(item)
        else:
            logger.warning(f"Skipped duplicate: {item_id}")
    return deduped

def replace_language_number_with_actual_value(language_number):
    list_of_language_ordered = ["hebrew","arabic","russian","french","english","amharic","spanish"]
    try:
        language_index = int(language_number) -1
        if language_index >= len(list_of_language_ordered) or language_index < 0:
            return "other"
        return list_of_language_ordered[language_index]
    except Exception as e:
        logger.warning(f"Failed to parse language number {language_number}: {e}")
    return "other"


## BRANCHES
def unwind_branches(ga: GuidestarAPI, stats: Stats):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows
        else:
            all_items = []  # Collect all branches first

            for _, row in enumerate(rows):
                regNum = row['id']
                branches = ga.branches(regNum)
                ids = [b['branchId'] for b in branches]
                if len(ids) != len(set(ids)):
                    logger.warning(f"Warning: duplicate branch IDs in fetched data {regNum}: {ids}")

                # Process branches
                for branch in branches:
                    ret = dict(row)  # copy row
                    data = dict()
                    data['name'] = branch.get('placeNickname') or f"{row.get('short_name') or row.get('name')} - {branch['cityName']}"
                    data['address'] = calc_address(branch)
                    data['location'] = calc_location_key(branch, data)
                    data['address_details'] = branch.get('drivingInstructions')
                    data['description'] = None
                    data['urls'] = None
                    data['phone_numbers'] = branch.get('phone')
                    data['organization'] = [regNum]

                    if branch.get('language'):
                        data['situations'] = [
                            f"human_situations:language:{replace_language_number_with_actual_value(l.lower().strip())}_speaking"
                            for l in branch['language'].split(';') if l != 8 and l != '8'
                        ]

                    ret['data'] = data
                    ret['id'] = 'guidestar:' + branch['branchId']
                    all_items.append(ret)

                # Organizations with no branches
                if not branches:
                    stats.increase('Entities: Org with no branches')
                    ret_list = list(ga.organizations(regNums=[regNum], cacheOnly=True))
                    if ret_list and ret_list[0]['data'].get('fullAddress'):
                        stats.increase('Entities: Org with no branches, used Guidestar official address')
                        data = ret_list[0]['data']
                        all_items.append(dict(
                            id='guidestar:' + regNum,
                            data=dict(
                                name=row['name'],
                                address=data['fullAddress'],
                                location=data['fullAddress'],
                                organization=[regNum]
                            )
                        ))
                    elif ret_list and row['kind'] not in ('עמותה', 'חל"צ', 'הקדש'):
                        stats.increase('Entities: Org with no branches, using org name as address')
                        cleaned_name = clean_org_name(row['name'])
                        all_items.append(dict(
                            id='budgetkey:' + regNum,
                            data=dict(
                                name=row['name'],
                                address=cleaned_name,
                                location=cleaned_name,
                                organization=[regNum]
                            )
                        ))

                # National entry
                national = dict(row)
                national['id'] = 'national:' + regNum
                disclaimer_text = ("שימו לב, ייתכן כי המיקום המוצג אינו מדויק וכי קיימים סניפים נוספים "
                                   "שבהם ניתן לקבל את השירות. מומלץ ליצור קשר ישירות עם הארגון לקבלת מידע מדויק ומעודכן.")
                existing_description = row.get('description', '')
                national['data'] = {
                    'organization': [regNum],
                    'name': '',
                    'address': 'שירות ארצי',
                    'location': 'שירות ארצי',
                    'description': f"{existing_description}\n\n{disclaimer_text}" if existing_description else disclaimer_text
                }
                all_items.append(national)

            # Deduplicate once at the top
            for item in deduplicate_items(all_items):
                yield item

    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
    )


def calc_address(row):
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
    if alternateAddress and alternateAddress != 'ללא כתובת':
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
        row.update(data)

    return func


def fetchBranchData(ga: GuidestarAPI, stats: Stats):
    print('FETCHING ALL ORGANIZATION BRANCHES')

    DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE,
                           settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='orgs'),
        DF.filter_rows(lambda r: r.get('source') == 'entities', resources='orgs'),
        DF.filter_rows(lambda r: r.get('status', '') == 'ACTIVE', resources='orgs'),
        # DF.filter_rows(lambda r: len(r.get('branches') or []) == 0, resources='orgs'),
        DF.select_fields(['id', 'name', 'short_name', 'kind'], resources='orgs'),
        DF.dump_to_path('temp/entities-orgs')
    ).process()

    airtable_updater(settings.AIRTABLE_BRANCH_TABLE, 'entities',
                     ['name', 'organization', 'address', 'address_details', 'location', 'description', 'phone_numbers',
                      'urls', 'situations'],
                     DF.Flow(
                         DF.load('temp/entities-orgs/datapackage.json'),
                         unwind_branches(ga, stats),
                     ),
                     updateBranchFromSourceData(),
                     airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE,
                     manage_status=False
                     )


## SERVICES
def process_service(row, taxonomies, rejected_taxonomies, stats: Stats):
    def update_from_taxonomy(names, responses, situations):
        for name in names:
            if name:
                try:
                    mapping = taxonomies[name]
                    responses.update(mapping['response_ids'] or [])
                    situations.update(mapping['situation_ids'] or [])
                except KeyError:
                    print('WARNING: no mapping for {}'.format(name))
                    stats.increase('Guidestar: Guidestar tag with no mapping')
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

    def rejected(names):
        for name in names:
            if str(name) in rejected_taxonomies:
                return True
        return False

    data = row['data']

    responses = set()
    situations = set()

    row['name'] = data.pop('serviceName')
    row['description'] = data.pop('voluntaryDescription') or data.pop('description')
    data_source_url = f'https://www.guidestar.org.il/organization/{data["organization_id"]}/services'
    row[
        'data_sources'] = f'מידע נוסף אפשר למצוא ב<a target="_blank" href="{data_source_url}">גיידסטאר - אתר העמותות של ישראל</a>'
    orgId = data.pop('organization_id')
    actual_branch_ids = data.pop('actual_branch_ids')
    row['branches'] = ['guidestar:' + b['branchId'] for b in (data.pop('branches') or []) if
                       b['branchId'] in actual_branch_ids]
    row['organizations'] = []

    record_type = data.pop('recordType')
    assert record_type == 'GreenInfo'
    for k in list(data.keys()):
        if k.startswith('youth'):
            data.pop(k)

    relatedMalkarService = data.pop('relatedMalkarService') or {}

    tags = []
    if 'serviceTypeNum' in data:
        tags.append(data.pop('serviceTypeNum'))
    if 'serviceTypeName' in data:
        tags.append(data.pop('serviceTypeName'))
    tags.extend((data.pop('serviceTargetAudience') or '').split(';'))
    tags.append('soproc:' + relatedMalkarService.get('serviceGovId', ''))
    if rejected(tags):
        stats.increase('Guidestar: Service with rejected tags')
        return None
    if 'נדרש סיוע' in (data.get('serviceName') or ''):
        stats.increase('Guidestar: Other "help needed" Service')
        return None
    if data.get('serviceId') == 'a0y0800000N15xoAAB':
        print('GOT SERVICE', data)
        print('HAS TAGS', tags, rejected_taxonomies)
    update_from_taxonomy(tags, responses, situations)

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
            national = True
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
            national = True
        elif item == 'Program':
            areas.append('תוכנית ייעודית בהרשמה מראש')
            national = True
        elif item in ('Not relevant', ''):
            pass
        else:
            assert False, 'area {}: {!r}'.format(area, row)

    if len(areas) > 1:
        details.append('השירות ניתן: ' + ', '.join(areas))
    elif len(areas) == 1:
        details.append('השירות ניתן ' + ''.join(areas))

    if national:
        row['branches'].append(f'national:{orgId}')
    if len(row['branches']) == 0:
        stats.increase('Guidestar: Service with no branches')
        return None

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

    startDate = data.pop('startDate', None)
    endDate = data.pop('endDate', None)
    if startDate:
        startDate = datetime.datetime.fromisoformat(startDate[:19]).date().strftime('%d/%m/%Y')
        details.append('תאריך התחלה: ' + startDate)
    if endDate:
        endDate = datetime.datetime.fromisoformat(endDate[:19]).date().strftime('%d/%m/%Y')
        details.append('תאריך סיום: ' + endDate)

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

    for k in ('isForCoronaVirus', 'lastModifiedDate', 'serviceId', 'isForBranch'):
        data.pop(k)
    row['situations'] = sorted(situations)
    row['responses'] = sorted(responses)
    assert all(v in (None, '0') for v in data.values()), repr(data_source_url) + ':' + repr(data)
    return dict(
        id=row['id'],
        data=row
    )


def unwind_services(ga: GuidestarAPI, taxonomies, rejected_taxonomies, stats: Stats):
    def func(rows: ResourceWrapper):
        if rows.res.name != 'orgs':
            yield from rows
        else:
            count = 0
            for _, row in enumerate(rows):
                regNum = row['id']

                branches = ga.branches(regNum)
                # if len(branches) == 0:
                #     continue
                services = ga.services(regNum)
                govServices = dict(
                    (s['relatedMalkarService'], s) for s in services if
                    s.get('serviceGovName') is not None and s.get('relatedMalkarService') is not None
                )
                for service in services:
                    if service['serviceId'] in govServices:
                        print('GOT RELATED SERVICE', service['serviceId'])
                        service['relatedMalkarService'] = govServices.get(service['serviceId'])
                    ret = dict()
                    ret.update(row)
                    ret['data'] = service
                    ret['data']['organization_id'] = regNum
                    ret['data']['actual_branch_ids'] = [b['branchId'] for b in branches]
                    ret['id'] = 'guidestar:' + service['serviceId']
                    ret = process_service(ret, taxonomies, rejected_taxonomies, stats)
                    if ret:
                        for k in ['source', 'status']:
                            ret['data'].pop(k, None)
                        count += 1
                        if count % 1000 == 0:
                            print('COLLECTED {} services: {}'.format(count, ret))
                        yield ret

    return DF.Flow(
        DF.add_field('data', 'object', resources='orgs'),
        func,
        DF.delete_fields(['source', 'status'], resources='orgs'),
    )


def fetchServiceData(ga, stats: Stats, taxonomies, rejected_taxonomies):
    print('FETCHING ALL ORGANIZATION SERVICES')

    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'guidestar',
                     ['name', 'description', 'details', 'payment_required', 'payment_details', 'urls', 'situations',
                      'responses',
                      'organizations', 'branches', 'data_sources', 'implements', 'phone_numbers', 'email_address'],
                     DF.Flow(
                         load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_ORGANIZATION_TABLE,
                                            settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
                         DF.update_resource(-1, name='orgs'),
                         DF.filter_rows(lambda r: r.get('status', '') == 'ACTIVE', resources='orgs'),
                         DF.select_fields(['id', 'name', 'source'], resources='orgs'),
                         unwind_services(ga, taxonomies, rejected_taxonomies, stats),
                         # DF.checkpoint('unwind_services'),
                     ),
                     update_mapper(),
                     # DF.Flow(
                     #     updateServiceFromSourceData(taxonomy, rejected_taxonomies, stats),
                     #     # lambda rows: (r for r in rows if 'drop' in r),
                     # ),
                     airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
                     )


def getGuidestarOrgs(ga: GuidestarAPI):
    today = datetime.date.today().isoformat()
    regNums = [
        dict(id=org['id'], data=dict(id=org['id'], last_tag_date=today))
        for org in ga.organizations()
    ]

    print('COLLECTED {} guidestar organizations'.format(len(regNums)))
    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'entities',
                     ['last_tag_date'],
                     regNums, update_mapper(),
                     manage_status=False,
                     airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
                     )


def scrapeGuidestarEntities(*_):
    logger.info('STARTING Entity + Guidestar Scraping')

    taxonomy = dict()
    print('FETCHING TAXONOMY MAPPING')
    taxonomy = DF.Flow(
        load_from_airtable(settings.AIRTABLE_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_GUIDESTAR_TABLE,
                           settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        # DF.printer(),
        DF.select_fields(['name', 'Status', 'situation_ids', 'response_ids']),
    ).results()[0][0]
    rejected_taxonomies = [x['name'] for x in taxonomy if 'REJECTED' == x.get('Status')]
    print('REJECTED TAXONOMIES', rejected_taxonomies)
    taxonomy = dict(
        (r.pop('name'), r) for r in taxonomy
        if r['name'] not in rejected_taxonomies
    )

    print('FETCHING SOPROC MAPPING')
    soproc_mappings = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATA_IMPORT_BASE, settings.AIRTABLE_TAXONOMY_MAPPING_SOPROC_TABLE,
                           settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.select_fields(['id', 'situation_ids', 'response_ids']),
    ).results()[0][0]
    taxonomy.update(dict(
        (r.pop('id'), r) for r in soproc_mappings
    ))

    stats = Stats()
    ga = GuidestarAPI()
    if (not isDebug):
        print('FETCHING Caches')
    ga.fetchCaches()
    if (not isDebug):
        print('FETCHING GUIDESTAR ORGANIZATIONS')
    getGuidestarOrgs(ga)
    if (not isDebug):
        print('FETCHING ORG DATA')
    fetchOrgData(ga, stats)
    if (not isDebug):
        print('FETCHING GUIDESTAR BRANCHES')
    fetchBranchData(ga, stats)
    if (not isDebug):
        print('FETCHING GUIDESTAR SERVICES')
    fetchServiceData(ga, stats, taxonomy, rejected_taxonomies)
    if (not isDebug):
        print('SAVING')
    stats.save()
    print('DONE')


def operator(*_):
    invoke_on(scrapeGuidestarEntities, 'Entities')


if __name__ == '__main__':
    scrapeGuidestarEntities(None, None, None)
