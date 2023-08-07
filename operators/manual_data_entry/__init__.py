import shutil
import datetime

import dataflows as DF
from slugify import slugify

from dataflows_airtable import load_from_airtable

from conf import settings
from srm_tools.hash import hasher
from srm_tools.logger import logger
from srm_tools.update_table import airtable_updater
from srm_tools.url_utils import fix_url
from ..derive.autocomplete import VERIFY_ORG_ID

CHECKPOINT = 'mde'

# PREPARE
def slugify_org_id():
    def func(row):
        id = row.get('Org Id') or row.get('Org Name')
        row['Org Id'] = slugify(id, separator='-', lowercase=True)
        if VERIFY_ORG_ID.match(row['Org Id']) is None:
            h = hasher(row['Org Id'])
            i = int(h, 16)
            row['Org Id'] = f'srm9{i}'
    return func

def handle_national_services():
    def func(row):
        if row.get('National Service?'):
            row['Branch Address'] = 'שירות ארצי'
    return func


def mde_prepare():
    DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.filter_rows(lambda r: (r.get('Org Id') or r.get('Org Name')) and r.get('Org Id') != 'dummy'),
        slugify_org_id(),
        handle_national_services(),
        DF.checkpoint(CHECKPOINT)
    ).process()

# ORGS
def org_updater():
    def func(row):
        if not row.get('data'):
            return
        data = row['data']
        row['name'] = data['name']
        row['short_name'] = row['short_name'] or data['short_name']
        if row.get('urls'):
            urls = row['urls'].split('\n')
        else:
            urls = []
        if data['urls']:
            new_urls = data['urls'].split('\n')
            for new_url in new_urls:
                new_url = fix_url(new_url)
                if new_url:
                    new_url = new_url + '#אתר הבית'
                    if new_url not in urls:
                        urls.append(new_url)
        row['urls'] = '\n'.join(urls)
        row['last_tag_date'] = data['last_tag_date']
    return func

def mde_organization_flow():
    today = datetime.date.today().isoformat()
    orgs = DF.Flow(
        DF.checkpoint(CHECKPOINT),
        DF.update_resource(-1, name='orgs'),
        DF.select_fields(['Org Id', 'Org Name', 'Org Short Name', 'Org Phone Number', 'Org Website']),
        DF.rename_fields({
            'Org Id': 'id',
            'Org Name': 'name',
            'Org Short Name': 'short_name',
            'Org Phone Number': 'phone_numbers',
            'Org Website': 'urls',
        }),
        DF.join_with_self('orgs', ['id'], dict(
            id=None, name=None, short_name=None, urls=None, phone_numbers=None
        )),
        DF.add_field('data', 'object', lambda r: dict(
            name=r['name'],
            short_name=r['short_name'],
            urls=r['urls'],
            phone_numbers=r['phone_numbers'],
            last_tag_date=today,
        )),
        DF.select_fields(['id', 'data']),
        DF.printer()
    ).results()[0][0]

    print('COLLECTED {} relevant organizations'.format(len(orgs)))

    airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'entities',
        ['name', 'short_name', 'urls', 'phone_numbers', 'last_tag_date'],
        orgs,
        org_updater(), 
        manage_status=False,
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )

# BRANCHES
def mde_id(*args):
    return 'mde:' + hasher(*map(str, args))

def branch_updater():
    def func(row):
        if not row.get('data'):
            return
        data = row['data']
        data['location'] = data['geocode'] or data['address']
        urls = []
        combined = []
        if data.get('urls'):
            urls = data['urls'].split('\n')
        for url in urls:
            url = fix_url(url)
            if url:
                combined.append(url + '#אתר הסניף')
        data['urls'] = '\n'.join(combined)
        row.update(data)

    return func


def mde_branch_flow():
    branches = DF.Flow(
        DF.checkpoint(CHECKPOINT),
        DF.update_resource(-1, name='branches'),
        DF.select_fields(['Org Id', 'Branch Details', 'Branch Address', 'Branch Geocode', 
                          'Branch Phone Number', 'Branch Email', 'Branch Website']),
        DF.rename_fields({
            'Org Id': 'organization',
            'Branch Details': 'name',
            'Branch Address': 'address',
            'Branch Geocode': 'geocode',
            'Branch Phone Number': 'phone_numbers',
            'Branch Email': 'email_address',
            'Branch Website': 'urls',
        }),
        DF.add_field('id', 'string', lambda r: mde_id(r['organization'], r['address'], r['geocode'])),
        DF.join_with_self('branches', ['id'], dict(
            id=None, 
            name=None, 
            address=None,
            geocode=None, 
            phone_numbers=None,
            email_address=None,
            urls=None,
            organization=None,
        )),
        DF.add_field('data', 'object', lambda r: dict(
            name=r['name'],
            address=r['address'],
            geocode=r['geocode'],
            phone_numbers=r['phone_numbers'],
            email_address=r['email_address'],
            urls=r['urls'],
            organization=[r['organization']],
        )),
        DF.select_fields(['id', 'data']),
    ).results()[0][0]

    print('COLLECTED {} relevant branches'.format(len(branches)))
    return airtable_updater(settings.AIRTABLE_BRANCH_TABLE, 'manual-data-entry',
        ['id', 'name', 'organization', 'location', 'address', 'phone_numbers', 'email_address', 'urls'],
        branches,
        branch_updater(),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )


# SERVICES
def service_updater():

    data_sources = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, 'DataReferences', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    ).results()[0][0]
    data_sources = dict((r['name'], r['reference']) for r in data_sources)

    def func(row):
        if not row.get('data'):
            return
        data = row['data']
        urls = []
        combined = []
        if data.get('urls'):
            urls = data['urls'].split('\n')
        for url in urls:
            url = fix_url(url)
            if url:
                combined.append(url + '#אתר השירות')
        data['urls'] = '\n'.join(combined)
        data['data_sources'] = data_sources.get(data['data_source'], '')
        row.update(data)

    return func

def mde_service_flow():

    # data_sources = DF.Flow(
    #     load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, 'DataReferences', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
    # ).results()[0][0]
    # data_sources = dict((r['name'], r['reference']) for r in data_sources)

    services = DF.Flow(
        DF.checkpoint(CHECKPOINT),
        DF.update_resource(-1, name='services'),
        DF.select_fields(['Org Id', 'Branch Address', 'Branch Geocode', 'Data Source',
                          'Service Name', 'Service Description', 'Service Conditions', 'Service Phone Number', 'Service Email', 'Service Website',
                          'responses_ids', 'situations_ids']),
        DF.rename_fields({
            'Org Id': 'organization',
            'Data Source': 'data_source',
            'Branch Address': 'branch_address',
            'Branch Geocode': 'branch_geocode',
            'Service Name': 'name',
            'Service Description': 'description',
            'Service Conditions': 'payment_details',
            'Service Phone Number': 'phone_numbers',
            'Service Email': 'email_address',
            'Service Website': 'urls',
        }),
        DF.add_field('branch_id', 'string', lambda r: mde_id(r['organization'], r['branch_address'], r['branch_geocode'])),
        DF.add_field('data', 'object', lambda r: dict(
            name=r['name'],
            description=r['description'],
            payment_details=r['payment_details'],
            urls=r['urls'],
            branches=[r['branch_id']],
            responses=r['responses_ids'],
            situations=r['situations_ids'],
            phone_numbers=r['phone_numbers'],
            data_source=r['data_source'],
            email_address=r['email_address'],
        )),
        DF.add_field('id', 'string', lambda r: mde_id(r['branch_id'], r['name'])),
        DF.select_fields(['id', 'data']),
    ).results()[0][0]

    print('COLLECTED {} relevant services'.format(len(services)))
    airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'manual-data-entry',
        ['id', 'name', 'description', 'payment_details', 'phone_numbers', 'email_address', 'urls', 'situations', 'responses', 'branches', 'data_sources'],
        services,
        service_updater(),
        airtable_base=settings.AIRTABLE_ENTITIES_IMPORT_BASE
    )


def operator(*_):
    logger.info('Starting Manual Data Entry Flow')
    
    shutil.rmtree(f'.checkpoints/{CHECKPOINT}', ignore_errors=True, onerror=None)
    
    mde_prepare()
    mde_organization_flow()
    mde_branch_flow()
    mde_service_flow()

    logger.info('Finished Manual Data Entry Flow')


if __name__ == '__main__':
    operator(None, None, None)
    # DF.Flow(revaha_branch_data_flow(),DF.printer()).process()
