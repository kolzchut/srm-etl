import hashlib
import re
from slugify import slugify
from pathlib import Path
import dataflows as DF

from conf import settings
from srm_tools.update_table import airtable_updater
from srm_tools.processors import update_mapper
from srm_tools.datagovil import fetch_datagovil_datastore
from srm_tools.stats import Stats
from srm_tools.error_notifier import invoke_on

FIELD_RENAME = {
    'name': 'clinic_name',
    'city': 'city',
    'age_group': 'audience',
    'intake_wait': 'intake_wait',
    'phone_numbers': 'phone',
    'expertise': 'specialization',
    'interventions': 'intervention_type',
    'street_address': 'street',
    'hmo': 'HMO_code',
}
HMOS = {
    1: 'לאומית',
    2: 'מכבי',
    3: 'כללית',
    4: 'מאוחדת',
    5: 'כל הקופות',
}
MISSING_VALUES = [
    'אין מומחיות מיוחדת',
    'לא קיים',
    'אין נתונים',
    'לא',
    'אין',
    'כל הקופות',
    'כל סוגי הטיפולים',
]
SITUATIONS = {
    'מבוגרים': [
        'human_situations:age_group:adults',
        'human_situations:age_group:young_adults',
        'human_situations:age_group:seniors',
    ],
    'טיפול יום-מבוגרים': [
        'human_situations:age_group:adults',
        'human_situations:age_group:young_adults',
        'human_situations:age_group:seniors',
    ],
    'מבוגרים-יועץ במרפאה ראשונית(ליאזון)': [
        'human_situations:age_group:adults',
        'human_situations:age_group:young_adults',
        'human_situations:age_group:seniors',
    ],
    'ילדים ונוער': [
        'human_situations:age_group:children',
        'human_situations:age_group:teens',
    ],
    'ילדים ונוער-יועץ במרפאה ראשונית(ליאזון)': [
        'human_situations:age_group:children',
        'human_situations:age_group:teens',
    ],
    'טיפול יום-נוער': [
        'human_situations:age_group:teens',
    ],
    'נוער': [
        'human_situations:age_group:teens',
    ]
}
ORGS = {
    'לאומית': dict(
        id='srm0010',
        data=dict(
            name='קופת חולים לאומית',
            short_name='לאומית',
            phone_numbers='1-700-507-507',
            urls='https://www.leumit.co.il/heb/Rights/mentalhealth/',
        ),
    ),
    'מכבי': dict(
        id='srm0011',
        data=dict(
            name='מכבי שירותי בריאות',
            short_name='מכבי',
            phone_numbers= '*3555',
            urls='https://www.maccabi4u.co.il/New/eligibilites/2062/',
        ),
    ),
    'כללית': dict(
        id='srm0012',
        data=dict(
            name='שירותי בריאות כללית',
            short_name='כללית',
            phone_numbers= '*2700',
            urls='https://www.clalit.co.il/he/your_health/family/mental_health/Pages/clalit_mental_health_clinics.aspx',
        ),
    ),
    'מאוחדת': dict(
        id='srm0013',
        data=dict(
            name='קופת חולים מאוחדת',
            short_name='מאוחדת',
            phone_numbers= '*3833',
            urls='https://www.meuhedet.co.il/%D7%9E%D7%90%D7%95%D7%97%D7%93%D7%AA-%D7%9C%D7%A0%D7%A4%D7%A9/'
        ),
    ),
    'default': dict(
        id='srm0019',
        data=dict(
            name='מרפאות בריאות נפש',
            short_name='משרד הבריאות',
            phone_numbers= '*5400',
            urls='https://www.health.gov.il/Subjects/mental_health/treatment/clinics/Pages/default.aspx',
        ),
    ),
}
DATA_SOURCE_ID = 'mental-health-clinics'
DATA_SOURCE_TEXT = 'המידע התקבל מ<a target="_blank" href="https://www.health.gov.il/Subjects/mental_health/treatment/clinics/Pages/mental-clinics.aspx">משרד הבריאות</a>'
splitter = re.compile('[.,\n]')
phone_number = re.compile('[0-9-]{7,}')


def description(row):
    fields = [
        ('interventions', 'סוגי התערבויות', 2),
        ('expertise', 'מומחיות המרפאה', 2),
        ('intake_wait', 'המתנה ממוצעת לאינטק (שבועות)', 0),
    ]
    ret = ''
    for f, title, min_len in fields:
        values = row[f]
        snippet = []
        for value in values:
            if value:
                value = splitter.split(value)
                value = [v.upper() for v in value if len(v) > min_len]
                snippet.extend(value)
        if len(snippet) > 0:
            ret += title + ': ' + ', '.join(set(snippet)) + '\n\n'
    return ret


def clinic_hash(row):
    items = [
        row['name'],
        row['phone_numbers'],
        row['address'],
        row['hmo']
    ]
    items = '|'.join(filter(None, items))
    return 'mhclinic-' + hashlib.sha1(items.encode('utf-8')).hexdigest()[:8]


def run(*_):
    stats = Stats()
    # Prepare data
    def ren(k, v):
        return DF.add_field(k, 'string', default=lambda row: row.pop(v, None))
    def del_data(row):
        for k in ['follow_up_wait', 'group_therapy_wait', 'individual_therapy_wait', ]:
            row.pop(k, None)
    DF.Flow(
        # Load and clean
        fetch_datagovil_datastore('mentalhealthclinics', 'מרפאות בריאות הנפש - משרד הבריאות'),
        DF.update_resource(-1, name='clinics'),
        *[ren(k, v) for k, v in FIELD_RENAME.items()],
        del_data,
        DF.set_type('hmo', transform=HMOS.get),
        DF.select_fields(FIELD_RENAME.keys(), resources=-1),
        DF.update_schema(-1, missingValues=MISSING_VALUES),
        DF.validate(on_error=DF.schema_validator.clear),

        # Filter out stuff
        stats.filter_with_stat('Mental Healtch Clinics: Not a clinic', lambda r: 'קליניקה' not in r['age_group'], resources=-1),
        stats.filter_with_stat('Mental Healtch Clinics: No address', lambda r: r['street_address'] is not None, resources=-1),

        # Prepare branch data
        DF.set_type('phone_numbers', transform=lambda v: '\n'.join(phone_number.findall(str(v))) if v else None, resources=-1),
        DF.set_type('intake_wait', type='string', transform=lambda v: str(v) if v else None, resources=-1),
        DF.add_field('address', 'string', lambda r: f'{r["street_address"]}, {r["city"]}' if r['city'] not in r['street_address'] else r['street_address'], resources=-1),
        DF.add_field('location', 'string', lambda r: r['address'].strip() if r['address'] else None, resources=-1),
        DF.delete_fields(['street_address', 'city'], resources=-1),

        DF.add_field('id', 'string', clinic_hash, resources=-1),

        DF.dump_to_path('temp/denormalized'),
        DF.printer()
    ).process()

    # Branches
    branches = DF.Flow(
        DF.load('temp/denormalized/datapackage.json'),
        # Join by branch id
        DF.join_with_self('clinics', ['id'], dict(
            id=None, name=None, address=None, location=None, hmo=None,
            phone_numbers=dict(aggregate='set'),
            interventions=dict(aggregate='set'),
            expertise=dict(aggregate='set'),
            intake_wait=dict(aggregate='set'),
        )),
        DF.add_field('description', 'string', description, resources=-1),
        DF.delete_fields(['interventions', 'expertise', 'intake_wait'], resources=-1),

        DF.set_type('phone_numbers', type='string', transform=lambda v: '\n'.join(filter(None, set('\n'.join(v).split('\n')))), resources=-1),

        # Constants
        DF.add_field('organization', 'string', lambda r: ORGS.get(r['hmo'] or 'default')['id'], resources=-1),
        DF.add_field('urls', 'string', lambda r: ORGS.get(r['hmo'] or 'default')['data']['urls'], resources=-1),

        DF.printer()
    ).results()[0][0]
    branches = [dict(id=b.pop('id'), data=b) for b in branches]

    # Services
    services = DF.Flow(
        DF.load('temp/denormalized/datapackage.json'),
        # Join by service name
        DF.set_type('name', transform=lambda _, row: 'מרפאת בריאות נפש ' + row['age_group'], resources=-1),
        DF.join_with_self('clinics', ['name'], dict(
            name=None,
            branches=dict(name='id', aggregate='set'),
            age_group=dict(aggregate='set'),
        )),
        DF.add_field('situations', 'array', lambda r: ['human_situations:disability:mental_illness'] + list(set(x for g in r['age_group'] for x in SITUATIONS[g])), resources=-1),
        DF.delete_fields(['age_group'], resources=-1),

        # Constants
        DF.add_field('responses', 'array', ['human_services:health:mental_health_care', 'human_services:place:health:clinic:mental_health_clinic'], resources=-1),
        DF.add_field('data_sources', 'string', DATA_SOURCE_TEXT, resources=-1),
        DF.add_field('id', 'string', lambda r: 'mhclinic-' + slugify(r['name']), resources=-1),

        DF.printer()
    ).results()[0][0]
    services = [dict(id=s.pop('id'), data=s) for s in services]

    # Organizations
    airtable_updater(
        settings.AIRTABLE_ORGANIZATION_TABLE,
        DATA_SOURCE_ID,
        ['name', 'short_name', 'phone_numbers'],
        ORGS.values(),
        update_mapper(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )

    airtable_updater(
        settings.AIRTABLE_BRANCH_TABLE,
        DATA_SOURCE_ID,
        ['name', 'address', 'location', 'description', 'phone_numbers', 'organization', 'urls'],
        branches,
        update_mapper(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )

    airtable_updater(
        settings.AIRTABLE_SERVICE_TABLE,
        DATA_SOURCE_ID,
        ['name', 'branches', 'situations', 'responses', 'data_sources'],
        services,
        update_mapper(),
        airtable_base=settings.AIRTABLE_DATA_IMPORT_BASE
    )

def operator(*_):
    invoke_on(run, 'Mental Health Clinics')

if __name__ == '__main__':
    operator(None, None, None)
