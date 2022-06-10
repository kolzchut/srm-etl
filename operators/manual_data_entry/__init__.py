import dataflows as DF
from slugify import slugify

from dataflows_airtable import load_from_airtable

from conf import settings
from srm_tools.logger import logger
from srm_tools.update_table import airtable_updater

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
                if new_url:
                    new_url = new_url.strip() + '#אתר הבית'
                    if new_url not in urls and new_url.startswith('http'):
                        urls.append(new_url)
        row['urls'] = '\n'.join(urls)
    return func

def mde_organization_flow():
    orgs = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='orgs'),
        DF.filter_rows(lambda r: r['Org Id'] and r['Org Id'] != 'dummy'),
        DF.select_fields(['Org Id', 'Org Name', 'Org Short Name', 'Org Website']),
        DF.rename_fields({
            'Org Id': 'id',
            'Org Name': 'name',
            'Org Short Name': 'short_name',
            'Org Website': 'urls',
        }),
        DF.join_with_self('orgs', ['id'], dict(
            id=None, name=None, short_name=None, urls=None,
        )),
        DF.add_field('data', 'object', lambda r: dict(
            name=r['name'],
            short_name=r['short_name'],
            urls=r['urls'],
        )),
        DF.select_fields(['id', 'data']),
    ).results()[0][0]

    print('COLLECTED {} relevant organizations'.format(len(orgs)))
    return airtable_updater(settings.AIRTABLE_ORGANIZATION_TABLE, 'entities',
        ['name', 'short_name', 'urls'],
        orgs,
        org_updater(), 
        manage_status=False
    )

# BRANCHES
def branch_id(org, address, geocode):
    return 'mde:' + org + ':' + slugify(geocode or address)

def branch_updater():
    def func(row):
        if not row.get('data'):
            return
        data = row['data']
        data['location'] = [data['geocode'] or data['address']]
        urls = []
        if data.get('urls'):
            urls = data['urls'].split('\n')
        org_urls = []
        if data.get('org_urls'):
            org_urls = data['org_urls'].split('\n')
        combined = []
        for url in urls:
            if url and url not in org_urls and url.startswith('http'):
                combined.append(url + '#אתר הסניף')
        data['urls'] = '\n'.join(combined)
        row.update(data)


    return func

def mde_branch_flow():
    branches = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='branches'),
        DF.filter_rows(lambda r: r['Org Id'] and r['Org Id'] != 'dummy' and r['Branch Details']),
        DF.select_fields(['Org Id', 'Branch Details', 'Branch Address', 'Branch Geocode', 'Branch Phone Number', 'Branch Email', 'Branch Website', 'Org Website']),
        DF.rename_fields({
            'Org Id': 'organization',
            'Branch Details': 'name',
            'Branch Address': 'address',
            'Branch Geocode': 'geocode',
            'Branch Phone Number': 'phone_numbers',
            'Branch Email': 'email_addresses',
            'Branch Website': 'urls',
            'Org Website': 'org_urls',
        }),
        DF.add_field('data', 'object', lambda r: dict(
            name=r['name'],
            address=r['address'],
            geocode=r['geocode'],
            phone_numbers=r['phone_numbers'],
            email_addresses=r['email_addresses'],
            urls=r['urls'],
            org_urls=r['org_urls'],
            organization=[r['organization']],
        )),
        DF.add_field('id', 'string', lambda r: branch_id(r['organization'], r['address'], r['geocode'])),
        DF.select_fields(['id', 'data']),
    ).results()[0][0]

    print('COLLECTED {} relevant branches'.format(len(branches)))
    return airtable_updater(settings.AIRTABLE_BRANCH_TABLE, 'manual-data-entry',
        ['id', 'name', 'organization', 'location', 'address', 'phone_numbers', 'email_addresses', 'urls'],
        branches,
        branch_updater(), 
    )


# SERVICES
def service_updater():
    def func(row):
        if not row.get('data'):
            return
        data = row['data']
        urls = []
        if data.get('urls'):
            urls = data['urls'].split('\n')
        org_urls = []
        if data.get('org_urls'):
            org_urls = data['org_urls'].split('\n')
        branch_urls = []
        if data.get('branch_urls'):
            branch_urls = data['branch_urls'].split('\n')
        combined = []
        for url in urls:
            if url and url not in org_urls and url not in branch_urls and url.startswith('http'):
                combined.append(url + '#אתר הסניף')
        data['urls'] = '\n'.join(combined)
        row.update(data)


    return func

def mde_service_flow():
    services = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, settings.AIRTABLE_SERVICE_TABLE, settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.update_resource(-1, name='services'),
        DF.filter_rows(lambda r: r['Org Id'] and r['Org Id'] != 'dummy' and r['Service Name']),
        DF.select_fields(['Org Id', 'Branch Address', 'Branch Geocode',
                          'Service Name', 'Service Description', 'Service Conditions', 'Service Phone Number', 'Service Email', 'Service Website',
                          'responses_ids', 'situations_ids', 'Org Website', 'Branch Website']),
        DF.rename_fields({
            'Org Id': 'organization',
            'Branch Address': 'branch_address',
            'Branch Geocode': 'branch_geocode',
            'Service Name': 'name',
            'Service Description': 'description',
            'Service Conditions': 'payment_details',
            'Service Phone Number': 'phone_numbers',
            'Service Email': 'email_addresses',
            'Service Website': 'urls',
            'Org Website': 'org_urls',
            'Branch Website': 'branch_urls',
        }),
        DF.add_field('branch_id', 'string', lambda r: branch_id(r['organization'], r['branch_address'], r['branch_geocode'])),
        DF.add_field('data', 'object', lambda r: dict(
            name=r['name'],
            description=r['description'],
            payment_details=r['payment_details'],
            urls=r['urls'],
            org_urls=r['org_urls'],
            branch_urls=r['branch_urls'],
            branches=[r['branch_id']],
            responses=r['responses_ids'],
            situations=r['situations_ids'],
        )),
        DF.add_field('id', 'string', lambda r: r['branch_id'] + ':' + slugify(r['name'])),
        DF.select_fields(['id', 'data']),
    ).results()[0][0]

    print('COLLECTED {} relevant branches'.format(len(services)))
    return airtable_updater(settings.AIRTABLE_SERVICE_TABLE, 'manual-data-entry',
        ['id', 'name', 'description', 'payment_details', 'urls', 'situations', 'responses', 'branches'],
        services,
        service_updater(), 
    )


def operator(*_):
    logger.info('Starting Manual Data Entry Flow')
    mde_organization_flow()
    mde_branch_flow()
    mde_service_flow()
    logger.info('Finished Manual Data Entry Flow')


if __name__ == '__main__':
    operator(None, None, None)
    # DF.Flow(revaha_branch_data_flow(),DF.printer()).process()
