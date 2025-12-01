from operators.child_care.fetch_as_df import fetch_as_df
from srm_tools.error_notifier import invoke_on


def run(*_):
    print("Hola")
    df = fetch_as_df()
    print(df.head(5))


def operator(*_):
    invoke_on(run, 'Day-Care')

if __name__ == '__main__':
    run(None, None, None)
