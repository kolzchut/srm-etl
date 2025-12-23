import pandas as pd
from typing import Optional, Union


def json_to_dataframe(data: Union[dict, list], records_key: Optional[str] = None) -> Optional[pd.DataFrame]:
    """
    Converts JSON data to a pandas DataFrame.

    :param data: JSON data (dict or list)
    :param records_key: Optional key if the list of records is nested in a dict
    :return: pandas DataFrame or None
    """
    if isinstance(data, list):
        return pd.DataFrame(data)

    if isinstance(data, dict):
        if records_key and records_key in data and isinstance(data[records_key], list):
            return pd.DataFrame(data[records_key])

        for key in ["records", "data", "items", "results"]:
            if key in data and isinstance(data[key], list):
                return pd.DataFrame(data[key])

        # Fallback: wrap the dict itself
        return pd.DataFrame([data])

    print("Unsupported JSON structure.")
    return None
