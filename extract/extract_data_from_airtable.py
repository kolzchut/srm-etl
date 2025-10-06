from typing import Optional
import pandas as pd
from pyairtable import Api
from srm_tools.logger import logger
from conf import settings

def load_airtable_as_dataframe(
    table_name: str,
    base_id: str,
    view: Optional[str] = None,
    api_key: Optional[str] = None,
) -> pd.DataFrame:
    """
    Load Airtable table as a pandas DataFrame (safe version).
    """
    api_key = api_key or settings.AIRTABLE_API_KEY
    api = Api(api_key)
    table = api.table(base_id, table_name)

    logger.info(f"Loading records from Airtable table '{table_name}' (base {base_id})...")

    batch = table.all(view=view)

    records = []
    for rec in batch:
        row = {"_airtable_id": rec["id"]}
        row.update(rec.get("fields", {}))
        records.append(row)

    df = pd.DataFrame(records)
    logger.info(f"Finished loading {len(df)} records from Airtable table '{table_name}'")
    return df
