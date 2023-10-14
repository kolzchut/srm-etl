import shutil

import dataflows as DF
import dataflows_airtable as DFA

from conf import settings

from .mde_utils import load_manual_data

CHECKPOINT = 'external-mde'

def fetch_google_spreadsheet():
    def func(rows):
        for row in rows:
            URL = row.get('Google Spreadsheet')
            if URL:
                services = DF.Flow(
                    DF.load(URL, headers=2, deduplicate_headers=True),
                    DF.filter_rows(lambda r: bool(r['שם השירות'])),
                    DF.filter_rows(lambda r: r['סטטוס'] == 'מוכן לפרסום'),
                    # DF.printer(),
                ).results()[0][0]
                for service in services:
                    emit = dict()
                    emit['Status'] = 'בייצור'
                    emit['Branch Address'] = service['כתובת או שם ישוב בו מסופק השירות'] or row['Branch Address']
                    emit['Branch Details'] = None
                    emit['Branch Geocode'] = None
                    emit['Branch Email'] = None
                    emit['Branch Website'] = None
                    emit['Branch Phone Number'] = None
                    emit['Service Name'] = service['שם השירות']
                    emit['Service Description'] = service['תיאור השירות']
                    emit['Service Conditions'] = service['אופן קבלת השירות']
                    emit['Service Phone Number'] = service['מספר טלפון תקני (1)']
                    emit['Service Website'] = service['דף אינטרנט']
                    emit['Service Email'] = service['אימייל (1)']
                    if service['שם המפעיל']:
                        emit['Org Name'] = service['שם המפעיל']
                        emit['Org Short Name'] = None
                        emit['Org Id'] = service['מספר תאגיד']
                        emit['Org Phone Number'] = service['מספר טלפון תקני (2)']
                        emit['Org Email'] = service['אימייל (2)']
                        emit['Org Website'] = service['אתר אינטרנט']
                    else:
                        emit['Org Name'] = row['Org Name']
                        emit['Org Short Name'] = row['Org Short Name']
                        emit['Org Id'] = row['Org Id']
                        emit['Org Phone Number'] = row.get('Org Phone Number')
                        emit['Org Email'] = row.get('Org Email')
                        emit['Org Website'] = row.get('Org Website')
                    emit['Data Source'] = row['Source Name']
                    emit['taxonomies'] = [service['קטגוריה'], service['אוכלוסיית יעד'], service['שפה']]
                    yield emit

    return DF.Flow(
        DF.add_field('Branch Email', 'string'),
        DF.add_field('Branch Website', 'string'),
        DF.add_field('Branch Phone Number', 'string'),
        DF.add_field('Service Name', 'string'),
        DF.add_field('Service Description', 'string'),
        DF.add_field('Service Conditions', 'string'),
        DF.add_field('Service Phone Number', 'string'),
        DF.add_field('Service Website', 'string'),
        DF.add_field('Service Email', 'string'),        
        DF.add_field('Data Source', 'string'),
        DF.add_field('taxonomies', 'array', []),
        func
    )

def handle_taxonomies(taxonomies):
    def func(row):
        responses = set()
        situations = set()
        for t in row['taxonomies']:
            if t in taxonomies:
                responses.update(taxonomies[t]['response_ids'] or [])
                situations.update(taxonomies[t]['situation_ids'] or [])
        row['responses_ids'] = list(responses)
        row['situations_ids'] = list(situations)

    return DF.Flow(
        DF.add_field('responses_ids', 'array', []),
        DF.add_field('situations_ids', 'array', []),
        func,
        DF.delete_fields(['taxonomies'])
    )


def main():
    data_sources = DF.Flow(
        DFA.load_from_airtable('app4yocYm963dR5Tt', 'Sources', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.select_fields(['Source Name', 'Source Legalese'])
    ).results()[0][0]
    data_sources = dict(
        (r['Source Name'], r['Source Legalese'])
        for r in data_sources
    )
    print(data_sources)
    taxonomies = DF.Flow(
        DFA.load_from_airtable('app4yocYm963dR5Tt', 'Categories', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.select_fields(['name', 'response_ids', 'situation_ids'])
    ).results()[0][0]
    taxonomies = dict(
        (r.pop('name'), r)
        for r in taxonomies
    )
    print(taxonomies)

    shutil.rmtree(f'.checkpoints/{CHECKPOINT}', ignore_errors=True, onerror=None)

    DF.Flow(
        DFA.load_from_airtable('app4yocYm963dR5Tt', 'Sources', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.filter_rows(lambda r: r['Status'] == 'בייצור'),
        fetch_google_spreadsheet(),
        DF.delete_fields([DFA.AIRTABLE_ID_FIELD, 'Status', 'Google Spreadsheet', 'Source Legalese', 'Source Name']),
        handle_taxonomies(taxonomies),
        DF.printer(),
        DF.dump_to_path('test', format='xlsx'),
        DF.checkpoint(CHECKPOINT)
    ).process()

    load_manual_data(DF.Flow(DF.checkpoint(CHECKPOINT)), data_sources, 'external-manual-data')


if __name__ == '__main__':
    main()