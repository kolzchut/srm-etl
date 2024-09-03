import dataflows as DF

from conf import settings
from srm_tools.logger import logger
from srm_tools.processors import update_mapper
from srm_tools.update_table import airtable_updater

# TODO: if/when we need a relational Data API
# need foreign key support, just for testing purposes now.
# def relational_sql_flow():
#     return DF.Flow(
#         DF.load(f'{settings.DATA_DUMP_DIR}/srm_data/datapackage.json'),
#         DF.dump_to_sql(
#             dict(
#                 srm_branches={'resource-name': 'branches'},
#                 srm_locations={'resource-name': 'locations'},
#                 srm_organizations={'resource-name': 'organizations'},
#                 srm_responses={'resource-name': 'responses'},
#                 srm_services={'resource-name': 'services'},
#                 srm_situations={'resource-name': 'situations'},
#             )
#         ),
#     )


def dump_to_sql_flow():
    return DF.Flow(
        DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
        DF.update_resource('card_data', name='cards'),
        DF.dump_to_sql(
            dict(
                cards={
                    'resource-name': 'cards',
                    'indexes_fields': [
                        ['service_name'],
                        ['organization_id'],
                        ['organization_kind'],
                        ['branch_city'],
                        ['national_service'],
                        ['card_id'],
                    ],
                }
            ), engine='env://DATASETS_DATABASE_URL'
        ),
    )

def cards_to_at_flow():

# card_id                          string
# situation_ids                    array
# response_ids                     array

# service_id                       string

# service_name                     string
# service_description                      string
# service_details                          string
# service_payment_required                         string
# service_payment_details                          string
# service_urls                     array
# service_phone_numbers                    array
# service_email_address                    string
# service_implements                       string
# service_boost                    number

# data_sources                     array

# organization_id                          string

# organization_name                        string
# organization_short_name                          string
# organization_description                         string
# organization_purpose                     string
# organization_kind                        string
# organization_urls                        array
# organization_phone_numbers                       array
# organization_email_address                       string
# organization_branch_count                        integer

# branch_id                        string
# branch_operating_unit                    string
# branch_description                       string
# branch_urls                      array
# branch_phone_numbers                     array
# branch_email_address                     string
# branch_address                   string
# branch_city                      string
# branch_geometry                          geopoint
# branch_location_accurate                         boolean

# national_service                         boolean

# score                    number

    # FIELDS = [
    #     'card_id', 'situation_ids', 'response_ids',
    #     'service_id',
    #     'service_name', 'service_description', 'service_details', 'service_payment_required', 'service_payment_details',
    #     'service_urls', 'service_phone_numbers', 'service_email_address', 'service_implements', 'service_boost',
    #     'data_sources',
    #     'organization_id',
    #     'organization_name', 'organization_short_name', 'organization_description', 'organization_purpose',
    #     'organization_kind', 'organization_urls', 'organization_phone_numbers', 'organization_email_address', 'organization_branch_count',
    #     'branch_id', 'branch_operating_unit', 'branch_description', 'branch_urls', 'branch_phone_numbers',
    #     'branch_email_address', 'branch_address', 'branch_city', 'branch_geometry', 'branch_location_accurate',
    #     'national_service',
    #     'score'
    # ]
    FIELDS = ['organization_id', 'service_id', 'branch_id', 'situation_ids', 'response_ids',
         'service_boost', 'organization_branch_count', 'branch_location_accurate']

    airtable_updater(
        settings.AIRTABLE_CARDS_TABLE, 'card',
        FIELDS, 
        DF.Flow(
            DF.load(f'{settings.DATA_DUMP_DIR}/card_data/datapackage.json'),
            DF.add_field('data', 'object', lambda r: dict((k, r.get(k)) for k in FIELDS), resources=-1),
            DF.add_field('id', 'string', lambda r: r.get('card_id'), resources=-1),
            DF.select_fields(['id', 'data'], resources=-1),
        ),
        update_mapper()
    )


def operator(*_):
    logger.info('Starting SQL Flow')

    # relational_sql_flow().process()
    # dump_to_sql_flow().process()
    cards_to_at_flow()

    logger.info('Finished SQL Flow')


if __name__ == '__main__':
    operator(None, None, None)
