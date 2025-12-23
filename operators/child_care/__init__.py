from operators.child_care.fetch_as_df import fetch_as_df
from operators.child_care.update_service import update_service
from srm_tools.error_notifier import invoke_on

def enrich_dataframe(df):
    df['source'] = 'meonot'
    df['status'] = 'ACTIVE'
    return df


def run(*_):
    df = fetch_as_df()
    df = enrich_dataframe(df)

    print(f'Updating Airtable...')

    modified_services=update_service(df)
    print(f"Effected {modified_services} services.")


def operator(*_):
    invoke_on(run, 'Child-Care')

if __name__ == '__main__':
    run(None, None, None)
