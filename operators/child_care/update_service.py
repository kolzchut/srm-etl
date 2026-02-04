import numpy as np
import pandas as pd
from conf import settings
from load.airtable import update_if_exists_if_not_create
from operators.meser.utilities.trigger_status_check import trigger_status_check
from utilities.update import prepare_airtable_dataframe
from srm_tools.hash import hasher

sector_to_situations = {
    "צרקסי": [
        "human_situations:sectors:circassians",
        "human_situations:age_group:infants",
    ],
    "ערבי": [
        "human_situations:sectors:arabs",
        "human_situations:language:arabic_speaking",
        "human_situations:age_group:infants",
    ],
    "בדואי": [
        "human_situations:sectors:bedouin",
        "human_situations:language:arabic_speaking",
        "human_situations:age_group:infants",
    ],
    "דרוזי": [
        "human_situations:sectors:druze",
        "human_situations:language:arabic_speaking",
        "human_situations:age_group:infants",
    ],
    "יהודי": ["human_situations:age_group:infants"]

}


def transform_dataframe_to_service(df) -> pd.DataFrame:
    df['id'] = df.apply(lambda row: 'meonot-' + hasher(str(row['שם וסמל מעון'])[-5:]), axis=1)
    df['data_sources'] = 'מידע נוסף אפשר למצוא ב<a target="_blank" href="https://parents.education.gov.il/prhnet/gov-education/kindergarten/search-daycare">פורטל ההורים באתר משרד החינוך</a>'
    df['name'] = 'מעון יום מסובסד לפעוטות בפיקוח משרד החינוך'
    df['description'] = 'מסגרת טיפולית חינוכית עבור פעוטות בגילים 0-3 המופעלת על ידי עמותות וגופים שקיבלו "סמל מעון" מהמדינה. המעון פועל במתכונת יום ארוך ומחויב להעניק סל שירותים הכולל סדר יום המכוון לקידום ההתפתחות הגופנית, השכלית והרגשית של הפעוט.'
    df['details'] = np.where(
        df['מגזר'] == 'יהודי',
        "",
        'המעון מיועד עבור החברה ה' + df['מגזר']
    )
    df['payment_details'] = 'שכר הלימוד במעונות היום נמצא תחת פיקוח, והוא נקבע לפי גיל הפעוט וסוג המסגרת. מידע נוסף ועדכני לגבי גובה שכר הלימוד ניתן למצוא ב<a target="_blank" href="https://www.gov.il/he/pages/tuition-daycare-and-supervised-nurseries>  אתר משרד העבודה</a>'
    df["situations"] = df["מגזר"].map(sector_to_situations)
    df['responses'] = [['human_services:care:daytime_care'] for _ in range(len(df))]
    df['payment_required'] = 'yes'
    df['payment_details'] = 'שכר הלימוד במעונות היום נמצא תחת פיקוח, והוא נקבע לפי גיל הפעוט וסוג המסגרת. מידע נוסף ועדכני לגבי גובה שכר הלימוד ניתן למצוא ב<a target="_blank" href="https://www.gov.il/he/pages/tuition-daycare-and-supervised-nurseries>  אתר משרד העבודה</a>'
    return df


def update_service(df):
    df = transform_dataframe_to_service(df)
    fields_to_prepare = ["id",'status',"source","data_sources","name", "description","details", "situations" ,"responses",'payment_required', 'payment_details']
    trigger_status_check(df=df, table_name=settings.AIRTABLE_SERVICE_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                         airtable_key_field='id', active_value='ACTIVE', inactive_value='INACTIVE',
                         only_from_source='meonot', df_key_field='id', batch_size=50)

    prepare_df = prepare_airtable_dataframe(df=df, fields_to_prepare=fields_to_prepare, key_field='id', airtable_key='id')

    modified = update_if_exists_if_not_create(df=prepare_df, airtable_key='id',
                                              base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                                              table_name=settings.AIRTABLE_SERVICE_TABLE, batch_size=50)
    return modified
