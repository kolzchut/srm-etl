import pandas as pd
from load.airtable import update_if_exists_if_not_create
from utilities.update import prepare_airtable_dataframe
from conf import settings
from operators.meser.utilities.get_foreign_key_by_field import get_foreign_key_by_field
from operators.meser.utilities.trigger_status_check import trigger_status_check
from srm_tools.logger import logger




def enrich_service_fields(df: pd.DataFrame) -> pd.DataFrame:
    df['source'] = 'meser'
    df['data_sources'] = "מידע על מסגרות רווחה התקבל ממשרד הרווחה והשירותים החברתיים"
    df['status'] = 'ACTIVE'
    return df



def update_airtable_services_from_df(df: pd.DataFrame) -> int:
    key_field = 'id'
    airtable_key = 'id'

    df.rename(columns={'service_id': 'id', 'service_name': 'name', "meser_id": "id_in_source"}, inplace=True)
    df = enrich_service_fields(df)
    trigger_status_check(df=df, table_name=settings.AIRTABLE_SERVICE_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                         airtable_key_field='id', active_value='ACTIVE', inactive_value='INACTIVE', only_from_source='meser', df_key_field='id', batch_size=50)


    # Link branches and organizations
    df = get_foreign_key_by_field(
        df=df,
        current_table=settings.AIRTABLE_SERVICE_TABLE,
        source_table=settings.AIRTABLE_BRANCH_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        base_field="branch_id",
        target_field="branches",
        airtable_key=airtable_key
    )


    fields_to_prepare = [
        'id', 'name', 'data_sources', 'situations', 'responses',
        'branches', 'id_in_source', 'source', 'status'
    ]
    df_prepared = prepare_airtable_dataframe(df, key_field, fields_to_prepare, airtable_key)

    if df_prepared.empty:
        logger.info("No service records to update.")
        return 0

    return update_if_exists_if_not_create(
        df=df_prepared,
        table_name=settings.AIRTABLE_SERVICE_TABLE,
        base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
        airtable_key=airtable_key
    )
