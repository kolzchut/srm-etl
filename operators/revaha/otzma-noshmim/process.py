import dataflows as DF

FILENAME = 'otzma-noshmim.xlsx'

if __name__ == '__main__':
    DF.Flow(
        DF.load(FILENAME),
        DF.select_fields([
            'semel_machlaka',
            'נושמים לרווחה',
            'מרכזי עוצמה',
        ]),
        DF.set_type('נושמים לרווחה', type='boolean', transform=lambda v: v == 'יש'),
        DF.set_type('מרכזי עוצמה', type='boolean', transform=lambda v: v == 'יש'),
        DF.rename_fields({
            'נושמים לרווחה': 'noshmim',
            'מרכזי עוצמה': 'otzma',
        }),
        DF.filter_rows(lambda r: r['semel_machlaka'] is not None),
        DF.dump_to_path('.'),
        DF.printer(),
    ).process()