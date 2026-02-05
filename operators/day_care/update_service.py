import pandas as pd

from conf import settings
from load.airtable import update_if_exists_if_not_create
from operators.meser.utilities.trigger_status_check import trigger_status_check
from utilities.update import prepare_airtable_dataframe


def add_static_records_to_df(df: pd.DataFrame) -> pd.DataFrame:
    df.loc[len(df)] = {
        'source': 'mol_daycare',
        'data_sources': 'מידע נוסף אפשר למצוא ב<a target="_blank" href="https://daycareclasssearch.labor.gov.il/">אתר משרד העבודה, פורטל איתור מסגרות מוכרות</a>',
        'name': 'משפחתון בפיקוח משרד העבודה',
        'description': 'מסגרת חינוכית-טיפולית לקבוצה של עד 5 פעוטות מגיל לידה ועד שלוש שנים. המשפחתונים מנוהלים בבית פרטי על-ידי מטפלות עצמאיות, ומפוקחים על-ידי משרד העבודה. ניתן לקבל סבסוד של שכר הלימוד ממשרד העבודה.',
        'details': 'שכר הלימוד במשפחתונים נמצא תחת פיקוח. מידע נוסף ועדכני לגבי גובה שכר הלימוד ניתן למצוא ב<a target="_blank" href="https://www.gov.il/he/pages/tuition-daycare-and-supervised-nurseries>אתר משרד העבודה</a>',
        'situations': ['human_situations:age_group:infants'],
        'responses': ['human_services:care:daytime_care'],
        'payment_required': 'yes',
        'id': 'mol_daycare-1',
        'status': 'ACTIVE'
    }

    df.loc[len(df)] = {
        'source': 'mol_daycare',
        'data_sources': 'מידע נוסף אפשר למצוא ב<a target="_blank" href="https://daycareclasssearch.labor.gov.il/">אתר משרד העבודה, פורטל איתור מסגרות מוכרות</a>',
        'name': 'צהרון לילדי גן',
        'description': 'מסגרת הפועלת במהלך בשעות אחר הצהריים, בהמשך ליום הלימודים בגני הילדים. במסגרת הצהרון מקבלים הילדים ארוחה חמה ומתקיימת פעילות העשרה. הצהרונים פועלים באישור משרד החינוך, וניתן לקבל סבסוד ממשרד העבודה',
        'details': '',
        'situations': ['human_situations:age_group:children'],
        'responses': ['human_services:education:afterschool_care'],
        'payment_required': 'yes',
        'id': 'mol_daycare-2',
        'status': 'ACTIVE'
    }
    return df




def update_service():
    fields_to_prepare = ['source', 'data_sources','name', 'description', 'details','situations','responses', 'payment_required','id','status']
    df = pd.DataFrame(columns=fields_to_prepare)
    df = add_static_records_to_df(df)

    trigger_status_check(df=df, table_name=settings.AIRTABLE_ORGANIZATION_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE,
                         airtable_key_field='id', active_value='ACTIVE', inactive_value='INACTIVE',
                         only_from_source='mol_daycare', df_key_field='id', batch_size=50)

    df_prepared = prepare_airtable_dataframe(df=df, key_field='id', fields_to_prepare=fields_to_prepare, airtable_key='id')

    if df_prepared.empty:
        print("No service records to update.")
        return 0

    modified = update_if_exists_if_not_create(df=df_prepared,table_name=settings.AIRTABLE_SERVICE_TABLE, base_id=settings.AIRTABLE_DATA_IMPORT_BASE, airtable_key='id')
    return modified
