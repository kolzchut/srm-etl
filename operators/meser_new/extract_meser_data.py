from extract.extract_data_from_api import fetch_json_from_api
from srm_tools.logger import logger
from transform.json_to_dataframe import json_to_dataframe


def datagovil_fetch_and_transform_to_dataframe(dataset_name: str = "welfare-frames",
                                               resource_name: str = "מסגרות רווחה"):
    """
    Fetches a dataset from data.gov.il and converts it into a pandas DataFrame.

    This function performs the following steps:
    1. Searches for the dataset by name using the Data.gov.il API.
    2. Filters the search results to find the dataset matching `dataset_name`.
    3. Attempts to locate the resource inside the dataset matching `resource_name`.
       If the specific resource is not found, the first resource in the dataset is used as a fallback.
    4. Determines the type of the resource:
       - If the resource has an 'id' field, it is assumed to be a Datastore resource.
         The function constructs a Datastore API URL and fetches records via JSON.
       - Otherwise, the function treats the resource as a direct JSON URL.
    5. Converts the JSON data into a pandas DataFrame using the `json_to_dataframe` helper function.
       The function automatically detects if the data is a list of records or nested under keys
       like "records", "data", "items", or "results".
    6. Prints the first few rows of the DataFrame and returns it.

    Notes:
    - The function sets pandas display options to show all columns when printing.
    - If the dataset or resource cannot be found, or if no data is returned, a warning is logged and None is returned.

    :param dataset_name: The dataset name on data.gov.il
    :param resource_name: The resource name within the dataset
    :return: pandas DataFrame containing the resource data, or None if failed.
    """

    dataset_search_url = f"https://data.gov.il/api/action/package_search?q={dataset_name}"
    dataset_info = fetch_json_from_api(dataset_search_url)

    if not dataset_info:
        logger.warn(f"Failed to fetch dataset info for '{dataset_name}'.")
        return None

    try:
        dataset = next(d for d in dataset_info["result"]["results"] if d["name"] == dataset_name)
    except StopIteration:
        logger.warn(f"Dataset '{dataset_name}' not found in search results.")
        return None

    try:
        resource = next(r for r in dataset["resources"] if r["name"] == resource_name)
    except StopIteration:
        resource = dataset["resources"][0]

    if "id" in resource:
        resource_id = resource["id"]
        datastore_url = f"https://data.gov.il/api/3/action/datastore_search?resource_id={resource_id}"
        records_data = fetch_json_from_api(datastore_url)
        df = json_to_dataframe(records_data.get("result", {}).get("records", []))
    else:
        df = json_to_dataframe(fetch_json_from_api(resource["url"]))

    if df is not None:
        return df
    else:
        logger.warn(f"No data returned for resource '{resource_name}'.")
        return None
