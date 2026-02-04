import pandas as pd


def match_organizations(fetched_df, fetched_field, airtable_df, airtable_field):
    """
    Matches organizations in fetched_df to airtable_df based on specific Hebrew prefixes
    and hyphenation logic. Handles Airtable list/array fields automatically.

    OVERRIDE LOGIC: If a match is found in Airtable, it overwrites the existing
    'organization_id' in fetched_df.
    """
    print("Matching organizations...")

    # Create a copy to avoid modifying the original dataframe passed in
    airtable_lookup = airtable_df.copy()

    def unpack_list(val):
        """
        Extracts the first item if val is a list (Airtable array),
        strips whitespace, and ensures it's a string.
        """
        if isinstance(val, list):
            return str(val[0]).strip() if len(val) > 0 else None
        if pd.isna(val):
            return None
        return str(val).strip()

    # 1. Prepare the lookup map: Unpack lists and normalize Hebrew spelling
    airtable_lookup[airtable_field] = airtable_lookup[airtable_field].apply(unpack_list)
    airtable_lookup = airtable_lookup.dropna(subset=[airtable_field])

    # Normalizing Airtable names to use double Yod 'עיריית' for consistency in the map
    def normalize_hebrew(name):
        if not name: return name
        return name.replace("עירית ", "עיריית ")

    # Create map: { Normalized_Name: ID }
    org_map = {normalize_hebrew(name): row_id for name, row_id in
               zip(airtable_lookup[airtable_field], airtable_lookup['id'])}

    # --- 2. Define Matching Logic ---
    def get_matching_org_id(row):
        org_name = row.get(fetched_field)

        if not isinstance(org_name, str) or not org_name:
            return None

        # Clean and Normalize input name
        clean_name = normalize_hebrew(org_name.strip())

        target_prefixes = ["מועצה מקומית", "מועצה אזורית", "עיריית"]
        matched_prefix = next((p for p in target_prefixes if clean_name.startswith(p)), None)

        # Attempt 1: Direct Match (handles cases without standard prefixes too)
        if clean_name in org_map:
            return org_map[clean_name]

        # If no prefix match, we can't do the hyphenation logic reliably
        if not matched_prefix:
            return None

        # Attempt 2: Hyphenation Logic (e.g., Beer Sheva vs Beer-Sheva)
        prefix_len = len(matched_prefix)
        city_part = clean_name[prefix_len:].strip()

        candidate_name = None
        if "-" in city_part:
            alt_city = city_part.replace("-", " ")
            candidate_name = f"{matched_prefix} {alt_city}"
        elif " " in city_part:
            alt_city = city_part.replace(" ", "-")
            candidate_name = f"{matched_prefix} {alt_city}"

        if candidate_name and candidate_name in org_map:
            return org_map[candidate_name]

        return None

    # --- 3. Apply Logic ---
    # We apply the matching logic to the fetched data
    matched_ids = fetched_df.apply(get_matching_org_id, axis=1)

    # --- 4. Override/Merge Logic ---
    if 'organization_id' not in fetched_df.columns:
        fetched_df['organization_id'] = None

    fetched_df['organization_id'] = matched_ids.combine_first(fetched_df['organization_id'])
    valid_ids = fetched_df['organization_id'].replace(0, None).replace('0', None)
    if 'ח.פ. ארגון' in fetched_df.columns:
        fetched_df['ח.פ. ארגון'] = valid_ids.combine_first(fetched_df['ח.פ. ארגון'])
    else:
        fetched_df['ח.פ. ארגון'] = valid_ids

    # Statistics
    match_count = matched_ids.notna().sum()
    print(f"Matched/Overridden {match_count} organizations based on prefix/hyphen logic.")

    return fetched_df
