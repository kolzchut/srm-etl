import os
import csv
from pyairtable import Api
from conf import settings
from srm_tools.logger import logger

def load_csv(meser_folder: str, encoding='cp1255'):
    """Load CSV and return list of rows."""
    csv_file_path = os.path.join(meser_folder, 'data.csv')
    if not os.path.exists(csv_file_path):
        logger.error(f"Meser CSV file not found: {csv_file_path}")
        return []
    with open(csv_file_path, 'r', encoding=encoding) as f:
        reader = csv.DictReader(f)
        return list(reader)

def get_airtable_table():
    """Return Airtable table object."""
    api = Api(settings.AIRTABLE_API_KEY)
    base_id = "appcmkagy4VbfIIC6"
    table_id = "tblgkJBrNp1ceLAKX"
    return api.table(base_id, table_id)

def build_record_map(table):
    """Map Registered_Business_Id -> Airtable record ID."""
    record_map = {}
    for record in table.all():
        fields = record.get('fields', {})
        business_id = fields.get('id')
        if business_id:
            record_map[business_id.strip()] = record['id']
    return record_map

def prepare_updates(rows, record_map):
    """Collect meser_ids per record as a set for batching."""
    updates_map = {}
    for row in rows:
        business_id = row.get('Registered_Business_Id')
        misgeret_id = row.get('Misgeret_Id')
        if business_id and misgeret_id:
            airtable_rec_id = record_map.get(business_id.strip())
            if airtable_rec_id:
                updates_map.setdefault(airtable_rec_id, set()).add(misgeret_id)
            else:
                logger.warning(f"Business ID '{business_id}' not found in Airtable")
    # Convert sets to comma-separated strings
    updates = [
        {"id": rid, "fields": {"meser_id": ",".join(sorted(mid_set))}}
        for rid, mid_set in updates_map.items()
    ]
    return updates

def batch_update_table(table, updates, batch_size=10):
    """Update Airtable table in batches, return modified count."""
    modified_count = 0
    for i in range(0, len(updates), batch_size):
        batch = updates[i:i + batch_size]
        try:
            table.batch_update(batch)
            modified_count += len(batch)
        except Exception as e:
            logger.error(f"Failed batch update: {e}")
    return modified_count

def update_organization_meser_id(meser_folder: str):
    """Main function to update organizations with meser_id."""
    logger.info("Loading CSV data...")
    rows = load_csv(meser_folder)
    if not rows:
        return

    logger.info("Getting Airtable table...")
    table = get_airtable_table()

    logger.info("Building record map...")
    record_map = build_record_map(table)

    logger.info("Preparing updates...")
    updates = prepare_updates(rows, record_map)

    logger.info("Performing batch updates...")
    modified_count = batch_update_table(table, updates)

    logger.info(f"Finished updating organizations. Total modified: {modified_count}")

