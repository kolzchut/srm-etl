import hashlib

# --- The Target IDs we want to crack ---
TARGET_BRANCH_HASH = 'dbdfa158'  # extracted from 'meser-dbdfa158'
TARGET_SERVICE_HASH = '0a5bf24b'  # extracted from 'meser-0a5bf24b'

# --- The Given Data ---
# Note: I've added arrays of 'candidates' to account for data cleaning 
# (strip, replace, concatenation) that might happen in the pipeline.

candidates = {
    'service_name': [
        'עלי שיח-ירושלים חרדי',
        'עלי שיח-ירושלים חרדי '.strip(),
    ],
    'address': [
        'הרב אבוהב 1 ירושלים',
        'הרב אבוהב 1  ירושלים',  # Double space
        'הרב אבוהב 1-ירושלים',  # Dash instead of space
        'הרב אבוהב 1 - ירושלים'.replace(' - ', '-'),  # Mimicking the code logic
        'הרב אבוהב 1',  # Partial
        'ירושלים',  # Partial
    ],
    'organization_id': [
        '580172831',
        580172831,  # As integer
        str(580172831)
    ],
    # You didn't provide a phone number, but the code uses it.
    # Since filter(None) is used, None and '' are treated as "skip this field".
    'phone_numbers': [
        None,
        '',
        '0',
    ]
}


# --- The Hasher Function (From your code) ---
def hasher(*args):
    # This filters out None or Empty strings, joins them, and hashes
    return hashlib.sha1(''.join(filter(None, map(str, args))).encode('utf-8')).hexdigest()[:8]


def run_bruteforce():
    print("--- Starting Brute Force ---")

    # ---------------------------------------------------------
    # STEP 1: Crack the Branch ID
    # Logic: branch_id = 'meser-' + hasher(address, organization_id)
    # ---------------------------------------------------------
    found_branch_id = None
    successful_branch_inputs = {}

    print(f"\n[1] Attempting to crack Branch ID (Target: {TARGET_BRANCH_HASH})...")

    for addr in candidates['address']:
        for org in candidates['organization_id']:
            # Calculate hash
            current_hash = hasher(addr, org)

            if current_hash == TARGET_BRANCH_HASH:
                print(f"✅ MATCH FOUND for Branch!")
                print(f"   Address used: '{addr}'")
                print(f"   Org ID used:  '{org}'")

                found_branch_id = 'meser-' + current_hash
                successful_branch_inputs = {'address': addr, 'org': org}
                break
        if found_branch_id: break

    if not found_branch_id:
        print("❌ Could not find a match for Branch ID with provided values.")
        return

    # ---------------------------------------------------------
    # STEP 2: Crack the Service ID
    # Logic: service_id = 'meser-' + hasher(service_name, phone_numbers, address, organization_id, branch_id)
    # ---------------------------------------------------------
    print(f"\n[2] Attempting to crack Service ID (Target: {TARGET_SERVICE_HASH})...")

    found_service = False

    for name in candidates['service_name']:
        for phone in candidates['phone_numbers']:
            # Use the address and org that worked in Step 1
            addr = successful_branch_inputs['address']
            org = successful_branch_inputs['org']

            # Calculate hash
            current_hash = hasher(name, phone, addr, org, found_branch_id)

            if current_hash == TARGET_SERVICE_HASH:
                print(f"✅ MATCH FOUND for Service!")
                print(f"   Service Name: '{name}'")
                print(f"   Phone used:   '{phone}' (Note: None/Empty means it was skipped)")
                print(f"   Address:      '{addr}'")
                print(f"   Org ID:       '{org}'")
                print(f"   Branch ID:    '{found_branch_id}'")
                found_service = True
                break
        if found_service: break

    if not found_service:
        print("❌ Could not find a match for Service ID.")
        print("   Did you forget to provide the 'Telephone' value? The hash depends on it.")


if __name__ == '__main__':
    run_bruteforce()
