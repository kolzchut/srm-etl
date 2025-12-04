import pandas as pd
from conf import settings
from load.airtable import get_airtable_table


def keep_needed_columns(df: pd.DataFrame, columns_to_keep: list) -> pd.DataFrame:
    """
    Returns a DataFrame containing only the specified columns.
    Ignores columns that do not exist in the source DataFrame to prevent KeyErrors.
    """
    valid_columns = [col for col in columns_to_keep if col in df.columns]

    missing_columns = set(columns_to_keep) - set(df.columns)
    if missing_columns:
        print(f"   Warning: The following requested columns were missing and skipped: {missing_columns}")

    return df[valid_columns].copy()


def loading_and_filtering_airtable_tables(original_base: str, test_base: str, tables: dict):
    dict_of_matching_tables = {}

    # CRITICAL FIX: Added .items() to iterate over key and value
    for table_name, columns_to_keep in tables.items():
        print(f"Comparing table: {table_name}")

        try:
            table_conn_original = get_airtable_table(table_name=table_name, base_id=original_base)

            raw_original = table_conn_original.all()
            records_original = [r['fields'] for r in raw_original]

            df_original = pd.DataFrame(records_original)

            table_conn_test = get_airtable_table(table_name=table_name, base_id=test_base)

            raw_test = table_conn_test.all()
            records_test = [r['fields'] for r in raw_test]

            df_test = pd.DataFrame(records_test)
        except Exception as e:
            print(f"Error loading {table_name}: {e}")
            continue

        if df_original.empty:
            print(f"Skipping {table_name}: 'original' DataFrame is empty.")
            continue
        if df_test.empty:
            print(f"Skipping {table_name}: 'test' DataFrame is empty.")
            continue

        required_cols = ["status", "source"]
        if not all(col in df_original.columns for col in required_cols):
            print(f"Skipping {table_name}: Missing required columns in original data.")
            continue

        for col in required_cols:
            if col not in df_test.columns:
                df_test[col] = None

        mask_original = df_original["status"].eq("ACTIVE") & df_original["source"].isin(["meser", "entities"])
        mask_test = df_test["status"].eq("ACTIVE") & df_test["source"].isin(["meser", "entities"])

        df_original_filtered = keep_needed_columns(df_original[mask_original].reset_index(drop=True), columns_to_keep)
        df_test_filtered = keep_needed_columns(df_test[mask_test].reset_index(drop=True), columns_to_keep)

        dict_of_matching_tables[table_name] = {"original": df_original_filtered, "test": df_test_filtered}

    return dict_of_matching_tables


def compare_and_export_differences(dict_of_matching_tables: dict):
    """
    Compares tables based on the 'id' column and exports differences to CSV.
    """
    print("\nStarting analysis of differences...")

    for table_name, data in dict_of_matching_tables.items():
        df_orig = data["original"]
        df_test = data["test"]

        # Ensure 'id' exists for comparison
        if 'id' not in df_orig.columns or 'id' not in df_test.columns:
            print(f"Skipping comparison for {table_name}: 'id' column missing.")
            continue

        # Set index to 'id' for easy lookup
        # drop_duplicates prevents errors if data has duplicate IDs
        df_orig = df_orig.drop_duplicates(subset=['id']).set_index('id')
        df_test = df_test.drop_duplicates(subset=['id']).set_index('id')

        all_ids = set(df_orig.index).union(set(df_test.index))
        differences = []

        for uid in all_ids:
            in_orig = uid in df_orig.index
            in_test = uid in df_test.index

            if in_orig and not in_test:
                # Record is only in Original - list all its values
                row = df_orig.loc[uid]
                for col in df_orig.columns:
                    differences.append({
                        "id": uid,
                        "type": "Only in Original",
                        "field": col,
                        "original_value": row[col],
                        "test_value": "Missing"
                    })
            elif not in_orig and in_test:
                # Record is only in Test - list all its values
                row = df_test.loc[uid]
                for col in df_test.columns:
                    differences.append({
                        "id": uid,
                        "type": "Only in Test",
                        "field": col,
                        "original_value": "Missing",
                        "test_value": row[col]
                    })
            else:
                # Exists in both, check fields
                row_orig = df_orig.loc[uid]
                row_test = df_test.loc[uid]

                # Only compare columns present in both filtered views
                common_columns = set(df_orig.columns).intersection(set(df_test.columns))

                for col in common_columns:
                    val_orig = row_orig[col]
                    val_test = row_test[col]

                    # Handle NaN comparison safely
                    is_diff = False
                    if pd.isna(val_orig) and pd.isna(val_test):
                        is_diff = False
                    elif pd.isna(val_orig) != pd.isna(val_test):
                        is_diff = True
                    elif val_orig != val_test:
                        is_diff = True

                    if is_diff:
                        differences.append({
                            "id": uid,
                            "type": "Field Difference",
                            "field": col,
                            "original_value": val_orig,
                            "test_value": val_test
                        })

        if differences:
            # Clean filename
            filename = f"{table_name.replace(' ', '_')}_differences.csv"
            pd.DataFrame(differences).to_csv(filename, index=False, encoding='utf-8-sig')
            print(f"Found differences in {table_name}. Exported to {filename}")
        else:
            print(f"{table_name}: No differences found.")


def run(*_):
    print("Starting Meser data comparison test...")

    original_base = "appcmkagy4VbfIIC6"
    test_base = "appOSY50gHa5M8Oex"

    tables_name_and_fields_to_compare = {
        settings.AIRTABLE_ORGANIZATION_TABLE: ['id', 'situations', 'phone_numbers'],
        settings.AIRTABLE_BRANCH_TABLE: ['id', 'name', 'organization', 'address', 'phone_numbers'],
        settings.AIRTABLE_SERVICE_TABLE: ['id', 'name', 'data_sources', 'situations', 'responses', 'branches']
    }

    dict_of_matching_tables = loading_and_filtering_airtable_tables(
        original_base,
        test_base,
        tables_name_and_fields_to_compare
    )

    compare_and_export_differences(dict_of_matching_tables)

    print("Completed Meser data comparison test.")


if __name__ == '__main__':
    run(None, None, None)
