import numpy as np

# Fix for NumPy 2.0 compatibility
if not hasattr(np, 'float_'):
    np.float_ = np.float64
import pandas as pd
import elasticsearch
from elasticsearch.helpers import scan
import os
from extract.extract_data_from_airtable import load_airtable_as_dataframe  # Adjust import path as needed

class Settings:
    ES_HOST = "srm-production-elasticsearch.whiletrue.industries"
    ES_PORT =443
    ES_HTTP_AUTH = "elastic:JI1EmP1UDxT9HYsmvWKJ"
    AIRTABLE_CARDS_TABLE = 'Cards'  # Adjust table name if needed
    AIRTABLE_PRODUCTION_BASE = 'appF3FyNsyk4zObNa'  # Production Base ID

settings = Settings()

# 1. Elasticsearch Connection Setup
def es_instance():
    """
    Creates the Elasticsearch client using secure settings (HTTPS/443).
    """
    return elasticsearch.Elasticsearch(
        [dict(
            host=settings.ES_HOST,
            port=int(settings.ES_PORT),
            scheme='https'  # Force HTTPS
        )],
        use_ssl=True,  # Enable SSL
        verify_certs=True,  # Verify server certificate
        timeout=60,
        retry_on_timeout=True,
        max_retries=3,
        retry_on_status=[502, 503, 504],
        **({"http_auth": settings.ES_HTTP_AUTH.split(':')} if settings.ES_HTTP_AUTH else {}),
    )


def find_active_cards_index():
    """
    Connects to ES, lists all indices, and returns the most recent one starting with 'srm__cards'.
    """
    client = es_instance()
    print("--- Connecting to Elasticsearch to find active index... ---")
    try:
        indices_raw = client.cat.indices(format="json")
    except Exception as e:
        print(f"CRITICAL ERROR Connecting to ES: {e}")
        return None

    # Filter for 'srm__cards' prefix
    candidates = [i['index'] for i in indices_raw if i['index'].startswith('srm__cards')]

    if not candidates:
        print("[!] No index starting with 'srm__cards' found.")
        # Debug: Print what WAS found
        print("Available indices:", [i['index'] for i in indices_raw])
        return None

    # Sort descending to get the newest timestamp first (e.g. srm__cards_2024...)
    candidates.sort(reverse=True)
    selected_index = candidates[0]

    print(f"---> Found {len(candidates)} candidate indices.")
    print(f"---> Selected Active Index: {selected_index}")
    return selected_index


def get_elasticsearch_dataframe(index_name, id_field='id'):
    """
    Fetches all documents from an ES index and returns them as a DataFrame.
    """
    client = es_instance()
    print(f"--- Fetching all records from Elasticsearch index: {index_name} ---")

    # Use scan to retrieve all documents (bypasses the 10k limit)
    query = {"query": {"match_all": {}}}

    cursor = scan(client, query=query, index=index_name)

    data = []
    for doc in cursor:
        source = doc.get('_source', {})
        record = source
        # Ensure we capture the ID.
        if id_field not in record:
            record[id_field] = doc['_id']
        data.append(record)

    df = pd.DataFrame(data)
    print(f"--- Fetched {len(df)} records from Elasticsearch ---")
    return df


def compare_datasets(airtable_df, es_df, id_col='id'):
    """
    Compares two dataframes on a specific ID column and returns 3 split dataframes.
    """
    # 1. Normalize IDs to strings to ensure matching works
    airtable_df[id_col] = airtable_df[id_col].astype(str).str.strip()
    es_df[id_col] = es_df[id_col].astype(str).str.strip()

    # 2. Identify Sets
    airtable_ids = set(airtable_df[id_col])
    es_ids = set(es_df[id_col])

    common_ids = airtable_ids.intersection(es_ids)
    only_airtable_ids = airtable_ids - es_ids
    only_es_ids = es_ids - airtable_ids

    # 3. Create DataFrames
    # A. Intersection (ID exists in both)
    df_common = pd.merge(
        airtable_df[airtable_df[id_col].isin(common_ids)],
        es_df[es_df[id_col].isin(common_ids)],
        on=id_col,
        how='inner',
        suffixes=('_airtable', '_es')
    )

    # B. Airtable Only
    df_airtable_only = airtable_df[airtable_df[id_col].isin(only_airtable_ids)].copy()

    # C. Elasticsearch Only
    df_es_only = es_df[es_df[id_col].isin(only_es_ids)].copy()

    return df_common, df_airtable_only, df_es_only


if __name__ == "__main__":
    # --- Configuration ---
    ID_COLUMN = 'card_id'

    # 1. Find the Dynamic Index Name
    ES_INDEX = find_active_cards_index()

    if ES_INDEX:
        # 2. Load Airtable Data
        print("\nLoading Airtable Data...")
        try:
            cards_airtable_df = load_airtable_as_dataframe(
                table_name=settings.AIRTABLE_CARDS_TABLE,
                base_id=settings.AIRTABLE_PRODUCTION_BASE
            )
            # Filter to ACTIVE status only
            cards_airtable_df = cards_airtable_df[cards_airtable_df['status'] == 'ACTIVE']
            # Ensure we have the comparison column
            if ID_COLUMN not in cards_airtable_df.columns and 'id' in cards_airtable_df.columns:
                print(f"Mapping 'id' to '{ID_COLUMN}' for Airtable data...")
                cards_airtable_df[ID_COLUMN] = cards_airtable_df['id']

            print(f"Airtable Records: {len(cards_airtable_df)}")
        except Exception as e:
            print(f"Failed to load Airtable data: {e}")
            cards_airtable_df = pd.DataFrame(columns=[ID_COLUMN])

        # 3. Load Elasticsearch Data
        print("\nLoading Elasticsearch Data...")
        try:
            cards_es_df = get_elasticsearch_dataframe(index_name=ES_INDEX, id_field=ID_COLUMN)
        except Exception as e:
            print(f"Failed to load ES data: {e}")
            cards_es_df = pd.DataFrame(columns=[ID_COLUMN])

        # 4. Perform Comparison
        if not cards_airtable_df.empty and not cards_es_df.empty:
            df_both, df_at_only, df_es_only = compare_datasets(
                cards_airtable_df,
                cards_es_df,
                id_col=ID_COLUMN
            )

            # 5. Report Results
            print("\n" + "=" * 30)
            print(" COMPARISON RESULTS ")
            print("=" * 30)
            print(f"1. IDs in BOTH: {len(df_both)}")
            print(f"2. IDs in Airtable ONLY (Missing in ES): {len(df_at_only)}")
            print(f"3. IDs in Elastic ONLY (Ghosts/Zombies): {len(df_es_only)}")

            # Save results if needed
            df_es_only.to_csv('ghosts_in_es.csv', index=False)
            df_at_only.to_csv('missing_in_es.csv', index=False)
            print("\nDetailed reports saved as 'ghosts_in_es.csv' and 'missing_in_es.csv'.")
        else:
            print("One or both dataframes are empty. Cannot perform comparison.")
