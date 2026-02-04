import pandas as pd

def filter_valid_rows(df: pd.DataFrame, fields: list) -> pd.DataFrame:
    """Keep rows where at least one field is not empty, None, NaN, or string 'None'."""
    def is_valid_row(row):
        for f in fields:
            v = row[f]
            if v is None:
                continue
            if isinstance(v, float) and pd.isna(v):
                continue
            if isinstance(v, str) and v.strip().lower() == 'none':
                continue
            return True
        return False

    return df[df.apply(is_valid_row, axis=1)]


def prepare_airtable_dataframe(df: pd.DataFrame, key_field: str, fields_to_prepare: list, airtable_key: str):
    """Filter existing columns, rename key, drop empty rows, and deduplicate."""
    # Only keep fields that exist in the dataframe
    existing_fields = [f for f in fields_to_prepare if f in df.columns]

    # Ensure key field is first and rename for Airtable
    df_for_update = df[[key_field] + [f for f in existing_fields if f != key_field]].copy()
    df_for_update = df_for_update.rename(columns={key_field: airtable_key})

    # Update fields list to reflect renamed key
    existing_fields = [airtable_key if f == key_field else f for f in existing_fields]

    # Drop empty/invalid rows
    df_for_update = filter_valid_rows(df_for_update, existing_fields)

    # Deduplicate by Airtable key (keep last)
    df_for_update = df_for_update.drop_duplicates(subset=[airtable_key], keep='last')

    return df_for_update
