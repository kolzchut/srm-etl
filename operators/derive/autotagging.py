import dataflows as DF
from dataflows_airtable import load_from_airtable

from conf import settings


def apply_auto_tagging():

    rules = DF.Flow(
        load_from_airtable(settings.AIRTABLE_DATAENTRY_BASE, 'Auto Tagging', settings.AIRTABLE_VIEW, settings.AIRTABLE_API_KEY),
        DF.rename_fields({
            'Query': 'query',
        }),
        DF.add_field('fields', 'array', lambda r: [x for x in [
            'organization_name' if r.get('In Org Name') else None,
            'organization_purpose' if r.get('In Org Purpose') else None,
            'service_name' if r.get('In Service Name') else None,
        ] if x]),
        DF.select_fields(['fields', 'query', 'situation_ids', 'response_ids']),
    ).results()[0][0]

    def func(rows):
        for row in rows:
            row['auto_tagged'] = row.get('auto_tagged') or []
            for rule in rules:
                found = False
                fields = rule['fields']
                query = rule['query']
                for field in fields:
                    value = row.get(field)
                    if value and isinstance(value, str):
                        if value.endswith(query) or query + ' ' in value:
                            found = True
                            break
                if found:
                    if rule['situation_ids']:
                        for s in rule['situation_ids']:
                            if s not in row['situation_ids']:
                                row['situation_ids'].append(s)
                            if s not in row['auto_tagged']:
                                row['auto_tagged'].append(s)
                    if rule['response_ids']:
                        for r in rule['response_ids']:
                            if r not in row['response_ids']:
                                row['response_ids'].append(r)
                            if r not in row['auto_tagged']:
                                row['auto_tagged'].append(r)
            yield row

    return DF.Flow(
        DF.add_field('auto_tagged', 'array'),
        func,
    )

if __name__ == '__main__':
    testing = DF.Flow(
        [
            {'organization_name': '1', 'organization_purpose': '2', 'service_name': '3', 'situation_ids': ['4'], 'response_ids': ['5']},        
            {'organization_name': 'האגודה למלחמה בסרטן', 'organization_purpose': '2', 'service_name': '3', 'situation_ids': ['4'], 'response_ids': ['5']},        
            {'organization_name': 'טיפול סרטןי', 'organization_purpose': '2', 'service_name': '3', 'situation_ids': ['4'], 'response_ids': ['5']},        
            {'organization_name': 'סרטן ריאות', 'organization_purpose': '2', 'service_name': '3', 'situation_ids': ['4'], 'response_ids': ['5']},        
            {'organization_name': '1', 'organization_purpose': '2', 'service_name': 'רק סרטן!', 'situation_ids': ['4'], 'response_ids': ['5']},        
        ],
        apply_auto_tagging(),
    ).results()[0][0]

    testing = [(x['situation_ids'], x['response_ids']) for x in testing]
    assert testing == [
        (['4'], ['5']),
        (['4', 'human_situations:health:neoplasm:cancer'], ['5']),
        (['4'], ['5']),
        (['4', 'human_situations:health:neoplasm:cancer'], ['5']),
        (['4'], ['5']),
    ], repr(testing)