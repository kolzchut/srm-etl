import dataflows as DF
import re
from slugify import slugify
from pathlib import Path

from conf import settings
from srm_tools.update_table import airflow_table_updater
from srm_tools.processors import update_mapper


FIELD_RENAME = {
    'שם המרפאה': 'orig_name',
    'ישוב': 'city',
    'כתובת': 'street_address',
    'טלפון': 'phone',
    'למבוטחי איזו קופה.+': 'hmo',
    'מבוגרים / ילדים': 'age_group',
    'סוגי התערבויות.+': 'interventions',
    'מומחיות המרפאה.+': 'expertise',
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
DATA_SOURCE_ID = 'mental-health-clinics'
splitter = re.compile('[.,\n]')
phone_number = re.compile('[0-9-]{7,}')


def description(row):
    fields = [
        ('interventions', 'סוגי התערבויות'),
        ('expertise', 'מומחיות המרפאה'),
    ]
    ret = ''
    for f, title in fields:
        values = row[f]
        snippet = []
        for value in values:
            if value:
                value = splitter.split(value)
                value = [v.upper() for v in value if len(v) > 2]
                snippet.extend(value)
        if len(snippet) > 0:
            ret += title + ': ' + ', '.join(set(snippet)) + '\n\n'
    return ret

FILENAME = Path(__file__).resolve().with_name('mentalhealthclinics.xlsx')


def operator(*_):
    # Prepare data
    DF.Flow(
        # Load and clean
        DF.load(str(FILENAME), name='clinics'),
        DF.select_fields(FIELD_RENAME.keys(), resources=-1),
        DF.rename_fields(FIELD_RENAME, resources=-1),
        DF.update_schema(-1, missingValues=MISSING_VALUES),
        DF.validate(on_error=DF.schema_validator.clear),

        # Filter out stuff
        DF.filter_rows(lambda r: 'קליניקה' not in r['age_group'], resources=-1),
        DF.filter_rows(lambda r: r['street_address'] is not None, resources=-1),

        # Prepare branch data
        DF.add_field('name', 'string', lambda r: f'{r["orig_name"]} - {r["hmo"]}' if r['hmo'] else r['orig_name'], resources=-1),
        DF.delete_fields(['orig_name', 'hmo'], resources=-1),

        DF.add_field('address', 'string', lambda r: f'{r["street_address"]}, {r["city"]}' if r['city'] not in r['street_address'] else r['street_address'], resources=-1),
        DF.add_field('location', 'array', lambda r: [r['address']], resources=-1),
        DF.delete_fields(['street_address', 'city'], resources=-1),

        DF.set_type('phone', transform=lambda v: ','.join(phone_number.findall(str(v))) if v else None, resources=-1),

        DF.add_field('id', 'string', lambda r: 'mhclinic-' + slugify(r['name']), resources=-1),
        DF.dump_to_path('temp/denormalized'),
        DF.printer()
    ).process()

    # Branches
    branches = DF.Flow(
        DF.load('temp/denormalized/datapackage.json'),
        # Join by branch name
        DF.join_with_self('clinics', ['name'], dict(
            id=None, name=None, address=None, location=None,
            phone=dict(aggregate='set'),
            interventions=dict(aggregate='set'),
            expertise=dict(aggregate='set'),
        )),
        DF.add_field('description', 'string', description, resources=-1),
        DF.delete_fields(['interventions', 'expertise'], resources=-1),

        DF.add_field('phone_numbers', 'string', lambda r: ','.join(r['phone']), resources=-1),

        # Constants
        DF.add_field('organization', 'string', 'mental-health-clinic'),

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
        DF.add_field('responses', 'array', ['human_services:health:mental_health_care'], resources=-1),
        DF.add_field('id', 'string', lambda r: 'mhclinic-' + slugify(r['name']), resources=-1),

        DF.printer()
    ).results()[0][0]
    services = [dict(id=s.pop('id'), data=s) for s in services]

    # Organizations
    organizations = [
        dict(
            id='mental-health-clinic',
            data=dict(
                name='מרפאת בריאות נפש',
            )
        )
    ]

    airflow_table_updater(
        settings.AIRTABLE_ORGANIZATION_TABLE,
        DATA_SOURCE_ID,
        ['name'],
        organizations,
        update_mapper(),
    )

    airflow_table_updater(
        settings.AIRTABLE_BRANCH_TABLE,
        DATA_SOURCE_ID,
        ['name', 'address', 'location', 'description', 'phone_numbers', 'organization'],
        branches,
        update_mapper(),
    )

    airflow_table_updater(
        settings.AIRTABLE_SERVICE_TABLE,
        DATA_SOURCE_ID,
        ['name', 'branches', 'situations', 'responses'],
        services,
        update_mapper(),
    )



if __name__ == '__main__':
    operator(None, None, None)