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
    1. Identifies records that do not match (exist in one but not the other) and exports them to *_unmatching_records.csv
    2. For records that DO match (exist in both), compares fields and exports differences to *_differences.csv
    """
    print("\nStarting analysis of differences...")

    for table_name, data in dict_of_matching_tables.items():
        print(f"\nProcessing {table_name}...")
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

        ids_orig = set(df_orig.index)
        ids_test = set(df_test.index)

        # --- STEP 1: Handle Unmatching Records ---
        only_in_orig = ids_orig - ids_test
        only_in_test = ids_test - ids_orig

        unmatching_records = []

        # Process records Only in Original
        if only_in_orig:
            # Select rows, reset index to get 'id' back as column
            df_unmatch_orig = df_orig.loc[list(only_in_orig)].reset_index()
            df_unmatch_orig['mismatch_type'] = 'Only in Original'
            unmatching_records.append(df_unmatch_orig)

        # Process records Only in Test
        if only_in_test:
            df_unmatch_test = df_test.loc[list(only_in_test)].reset_index()
            df_unmatch_test['mismatch_type'] = 'Only in Test'
            unmatching_records.append(df_unmatch_test)

        # Export Unmatching CSV
        if unmatching_records:
            df_unmatching = pd.concat(unmatching_records, ignore_index=True)

            # Reorder columns to put 'id' and 'mismatch_type' at the start
            cols = ['id', 'mismatch_type'] + [c for c in df_unmatching.columns if c not in ['id', 'mismatch_type']]
            df_unmatching = df_unmatching[cols]

            unmatch_filename = f"{table_name.replace(' ', '_')}_unmatching_records.csv"
            df_unmatching.to_csv(unmatch_filename, index=False, encoding='utf-8-sig')
            print(f"-> Found {len(df_unmatching)} unmatching records. Exported to {unmatch_filename}")
        else:
            print(f"-> All records match by ID (no unmatching records).")

        # --- STEP 2: Compare Fields for Common Records ---
        common_ids = ids_orig.intersection(ids_test)
        field_differences = []

        if common_ids:
            # Create subsets of only common IDs for faster iteration
            df_orig_common = df_orig.loc[list(common_ids)]
            df_test_common = df_test.loc[list(common_ids)]

            common_columns = set(df_orig.columns).intersection(set(df_test.columns))

            for uid in common_ids:
                row_orig = df_orig_common.loc[uid]
                row_test = df_test_common.loc[uid]

                for col in common_columns:
                    val_orig = row_orig[col]
                    val_test = row_test[col]

                    is_diff = False

                    # 1. Handle Lists (Airtable Linked Records/Multi-selects)
                    if isinstance(val_orig, list) or isinstance(val_test, list):
                        if type(val_orig) != type(val_test):
                            is_diff = True
                        else:
                            # Both are lists. Compare directly.
                            is_diff = val_orig != val_test

                    # 2. Handle Scalars
                    else:
                        if pd.isna(val_orig) and pd.isna(val_test):
                            is_diff = False
                        elif pd.isna(val_orig) != pd.isna(val_test):
                            is_diff = True
                        elif val_orig != val_test:
                            is_diff = True

                    if is_diff:
                        field_differences.append({
                            "id": uid,
                            "type": "Field Difference",
                            "field": col,
                            "original_value": val_orig,
                            "test_value": val_test
                        })

        # Export Differences CSV
        if field_differences:
            diff_filename = f"{table_name.replace(' ', '_')}_differences.csv"
            pd.DataFrame(field_differences).to_csv(diff_filename, index=False, encoding='utf-8-sig')
            print(f"-> Found {len(field_differences)} field differences in common records. Exported to {diff_filename}")
        else:
            print(f"-> No field differences found in common records.")


def run(*_):
    print("Starting Meser data comparison test...")

    original_base = "appcmkagy4VbfIIC6"
    test_base = "appdGUt9ruwdyU4tk"

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
