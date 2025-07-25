import json
import os
import sys
import csv
from datetime import datetime
import logging
import requests
from typing import List, Dict, Any, Optional, Union
import pandas as pd
from pythonjsonlogger import jsonlogger
from okta_sync_utils import (
    get_request,
    get_general_credentials,
    upload_dataframe_to_bigquery,
    download_from_bigquery_as_dataframe,
    query_bigquery_as_dataframe,
    upload_to_gcs,
    dbt_run,
)


class SingletonConfig:
    """
    A singleton class that provides global configuration settings for the application.

    Attributes:
        _instance (SingletonConfig): The singleton instance of the class.
        _folder_path (str): The path to the log folder.
        _project_id (str): The ID of the BigQuery project.
        _dataset_id (str): The ID of the temporary dataset in BigQuery.
        _target_dataset_id (str): The ID of the target dataset in BigQuery.
    """

    _instance = None
    _folder_path = None
    _project_id = None
    _dataset_id = None
    _target_dataset_id = None

    def __new__(cls):
        """
        Creates a new instance of the SingletonConfig class if one does not already exist.

        Returns:
            SingletonConfig: The singleton instance of the class.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._folder_path = None
            cls._instance._project_id = "cru-data-warehouse-elt-prod"
            cls._instance._dataset_id = "temp_okta"
            # cls._instance._target_dataset_id = "temp_okta2"  # for testing
            cls._instance._target_dataset_id = "el_okta"
        return cls._instance

    def create_log_folder(self):
        """
        Creates a log folder with the starting date as the folder name.
        """
        if self._folder_path is None:
            current_folder = os.path.dirname(os.path.abspath(__file__))
            sub_folder = datetime.now().strftime("%Y-%m-%d")
            self._folder_path = os.path.join(current_folder, sub_folder)
            os.makedirs(self._folder_path, exist_ok=True)

    @property
    def log_path(self) -> str:
        """
        Returns the path to the log folder.

        Returns:
            str: The path to the log folder.

        Raises:
            ValueError: If the folder path has not been created yet.
        """
        if self._folder_path is not None:
            return self._folder_path
        else:
            raise ValueError("Folder path has not been created yet")

    @property
    def project_id(self) -> str:
        """
        Returns the project_id of the BigQuery project.

        Returns:
            str: The project_id of the BigQuery project.

        Raises:
            ValueError: If the project_id has not been set yet.
        """
        if self._project_id is not None:
            return self._project_id
        else:
            raise ValueError("Project ID has not been set yet")

    @property
    def dataset_id(self) -> str:
        """
        Returns the project_id of the temp dataset in BigQuery.

        Returns:
            str: The project_id of the temp dataset in BigQuery.

        Raises:
            ValueError: If the temp project_id has not been set yet.
        """
        if self._dataset_id is not None:
            return self._dataset_id
        else:
            raise ValueError("Dataset ID has not been set yet")

    @property
    def target_dataset_id(self) -> str:
        """
        Returns the project_id of the target dataset in BigQuery.

        Returns:
            str: The project_id of the target dataset in BigQuery.

        Raises:
            ValueError: If the target project_id has not been set yet.
        """
        if self._target_dataset_id is not None:
            return self._target_dataset_id
        else:
            raise ValueError("Target Dataset ID has not been set yet")


def setup_logging() -> None:
    """
    Sets up logging for the application.

    This function creates a log folder with the starting date as the folder name, and sets up two logging handlers:
    one that writes log messages to a file in the log folder,
    and another that writes log messages to the console/stdout which will end up in Cloud Run Logs.
    The log messages are formatted as JSON objects with the following keys:
    - asctime: The time the log message was created, in UTC.
    - status: The logging level (levelname) of the message (e.g., INFO, WARNING, ERROR).
    - message: The log message itself.

    Raises:
        ValueError: If the log folder path has not been created yet.
    """
    ci = SingletonConfig()
    ci.create_log_folder()
    file_name = "output.log"
    logfile = os.path.join(ci.log_path, file_name)
    json_formatter = jsonlogger.JsonFormatter("%(asctime)s %(levelname)s %(message)s")
    file_handler = logging.FileHandler(logfile)
    console_handler = logging.StreamHandler(stream=sys.stdout)
    file_handler.setLevel(logging.INFO)
    console_handler.setLevel(logging.INFO)
    file_handler.setFormatter(json_formatter)
    console_handler.setFormatter(json_formatter)
    logger = logging.getLogger("primary_logger")
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    # Set the custom excepthook function to handle unhandled exceptions
    sys.excepthook = handle_unhandled_exception


def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    """
    Handles unhandled exceptions by logging the exception details.

    This function is intended to be used as a custom excepthook function, which is called when an unhandled exception
    occurs in the application. The function logs the exception details to the primary logger.

    Args:
        exc_type (type): The type of the exception that was raised.
        exc_value (Exception): The exception object that was raised.
        exc_traceback (traceback): The traceback object that was generated when the exception was raised.
    """
    # Check if the exception is of a type that can be skipped (e.g., KeyboardInterrupt)
    if issubclass(exc_type, KeyboardInterrupt):
        sys.__excepthook__(exc_type, exc_value, exc_traceback)
        return
    # Log the unhandled exception
    logger = logging.getLogger("primary_logger")
    logger.exception(
        "Unhandled exception", exc_info=(exc_type, exc_value, exc_traceback)
    )


def get_data(
    endpoint: str, url: str, headers: Dict[str, str], params: Dict[Any, Any]
) -> pd.DataFrame:
    """
    Retrieves data from the specified Okta API endpoint and returns it as a pandas DataFrame.

    This function retrieves data from an API endpoint by sending a GET request to the specified URL with the specified
    headers and parameters. The function then concatenates the data from all pages of the response into a single
    pandas DataFrame.

    Args:
        endpoint (str): The name of the API endpoint being queried.
        url (str): The URL of the API endpoint.
        headers (Dict[str, str]): A dictionary of headers to include in the GET request.
        params (Dict[Any, Any]): A dictionary of parameters to include in the GET request.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the data retrieved from the API endpoint.

    Raises:
        ValueError: If the API endpoint returns an error response.
    """
    logger = logging.getLogger("primary_logger")
    page_count = 1
    logger.info(f"Downloading {endpoint} on page {page_count} from {url}")
    r = get_request(url, headers, params)
    if isinstance(r, requests.Response):
        data = r.json()
        links = r.links
        df = pd.DataFrame(data)
        while "next" in links:  # comment out this while loop for testing
            page_count += 1
            url = links["next"]["url"]
            logger.info(f"Downloading {endpoint} on page {page_count} from {url}")
            params = {None: None}
            r = get_request(url, headers, params)
            if isinstance(r, requests.Response):
                data_next = r.json()
                links = r.links
                df_next = pd.DataFrame(data_next)
                df = pd.concat([df, df_next], axis=0, ignore_index=True)
            else:
                break
        logger.info(f"All {endpoint} downloaded successfully.")
        return df
    else:
        return pd.DataFrame()


def get_all_users(
    endpoint: str,
    headers: Dict[str, str],
    params: Dict[Any, Any],
    ids: List[str],
    columns: List[str],
) -> pd.DataFrame:
    """
    Downloads user data for all provided app or group ids.

    This function downloads app users or group members data by looping through all provided app ids or group ids.
    The function then concatenates the data from all pages of the response into a single
    pandas DataFrame.

    Args:
        endpoint (str): The Okta API endpoint for the group data.
        headers (Dict[str, str]): A dictionary of headers to include in the GET request.
        params (Dict[Any, Any]): A dictionary of parameters to include in the GET request.
        ids (List[str]): A list of group_ids or app_ids for which to download user data.
        columns (List[str]): A list of column names to include in the downloaded data.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the user data.

    Raises:
        requests.exceptions.HTTPError: If the server returns an HTTP error status code.
        requests.exceptions.Timeout: If the request times out.
        requests.exceptions.ConnectionError: If a connection error occurs while sending the request.
        requests.exceptions.RequestException: If a general error occurs while sending the request.
    """
    logger = logging.getLogger("primary_logger")

    id_count = 0
    id_count_total = len(ids)
    df = pd.DataFrame()
    for id in ids:
        id_count += 1
        page_count = 1
        url = f"https://signon.okta.com/api/v1/{endpoint}/{id}/users?"
        params_copy = params.copy()
        while True:
            logger.info(
                f"Downloading {endpoint[:-1]} users for {endpoint[:-1]} {id} ({id_count}/{id_count_total}) on page {page_count} from {url}"
            )
            r = get_request(url, headers, params_copy)
            if isinstance(r, requests.Response):
                users = r.json()
                links = r.links
                for user in users:
                    user[f"{endpoint[:-1]}_id"] = id
                df_current = pd.DataFrame(users)
                df_current = df_current.reindex(columns, axis=1)
                df = pd.concat([df, df_current], axis=0, ignore_index=True)
                if "next" in links:
                    page_count += 1
                    url = links["next"]["url"]
                    params_copy = {None: None}
                else:
                    break
            else:
                break
    logger.info(f"All {endpoint} users downloaded successfully.")
    return df


def get_schema(table_id: str) -> Union[Dict[Any, Any], None]:
    """
    Retrieves the schema for the specified table from local file.

    This function retrieves the schema for the specified table by reading the schema JSON file associated with the table.
    The function then returns the resulting schema as a dictionary.

    Args:
        table_id (str): The ID of the table for which to retrieve the schema.

    Returns:
        Dict[str, Any] or None: A dictionary representing the schema for the specified table or None if failed to retrieve the schema.

    Raises:
        FileNotFoundError: If the schema file for the specified table cannot be found.
    """
    logger = logging.getLogger("primary_logger")
    current_folder = os.path.dirname(os.path.abspath(__file__))
    subfolder = "schemas"
    file_name = f"{table_id}_schema.json"
    file_path = os.path.join(current_folder, subfolder, file_name)
    try:
        with open(file_path) as file:
            schema_json = json.load(file)
        logger.info("Retrieved schemas from local files.")
        return schema_json
    except Exception as e:
        logger.error(f"Retrieve Schema from file failed: {str(e)}")
        return None


def match_schema(df: pd.DataFrame, schema_json: Dict[Any, Any]) -> pd.DataFrame:
    """
    Matches the schema of a pandas DataFrame to a specified schema and converts data types as necessary.

    This function matches the schema of a pandas DataFrame to a specified schema by converting the data types of the
    DataFrame columns to match the corresponding types in the schema. The function then returns the resulting DataFrame.

    Args:
        df (pd.DataFrame): The pandas DataFrame to match to the schema.
        schema_json (Dict[Any, Any]): A dictionary representing the schema to match the DataFrame to.

    Returns:
        pd.DataFrame: A pandas DataFrame with the same schema as the specified schema and with data types converted as necessary.

    """
    logger = logging.getLogger("primary_logger")
    dtypes = {}
    for field in schema_json:
        # Check if the column exists in the DataFrame
        if field["name"] in df.columns:
            # convert df fields to datetime if the field type is TIMESTAMP
            if field["type"] == "TIMESTAMP":
                df[field["name"]] = pd.to_datetime(df[field["name"]])
            elif field["type"] == "INTEGER":
                dtypes[field["name"]] = "Int64"
            # convert json string to python string
            else:
                dtypes[field["name"]] = field["type"].lower()
        else:
            # Add missing column with empty values
            if field["type"] == "TIMESTAMP":
                df[field["name"]] = pd.to_datetime(pd.Series([] * len(df)))
            else:
                df[field["name"]] = pd.Series([] * len(df), dtype=field["type"].lower())
    df = df.astype(dtypes)
    # Create a list of all schema column names
    schema_columns = [field["name"] for field in schema_json]
    # Drop any columns in `df` that are not in the `schema_columns` list
    df = df.loc[:, df.columns.isin(schema_columns)]  # type: ignore
    logger.info("Prepared data for uploading.")
    return df


def replace_dataset_bigquery() -> None:
    """
    Updates tables in the target dataset from the temporary dataset in BigQuery.

    This function updates tables in the target dataset from the temporary dataset in BigQuery by creating or replacing
    each table in the target dataset with the corresponding table in the temporary dataset. The function logs each table
    that is updated, and logs a message when the update is complete.

    Args:
        None

    Returns:
        None

    """
    logger = logging.getLogger("primary_logger")
    ci = SingletonConfig()
    project_id = ci.project_id
    dataset_id = ci.dataset_id
    target_dataset_id = ci.target_dataset_id
    tables = [
        "okta_apps",
        "okta_groups",
        "okta_users",
        "okta_group_members",
        "okta_app_users",
        "okta_everyone_group_ids",
        "okta_everyone_app_ids",
    ]
    for table in tables:
        qry_replace = f"""
        create or replace table {project_id}.{target_dataset_id}.{table} as
        select * from {project_id}.{dataset_id}.{table};
        """
        logger.info(f"Updating BigQuery table: {target_dataset_id}.{table}")
        query_bigquery_as_dataframe(qry_replace, "CRU_DATA_WAREHOUSE_ELT_PROD")
    logger.info(
        f"Target dataset {target_dataset_id} updated with latest data from: {dataset_id}."
    )


def get_new_everyone_ids_bigquery(table_id: str) -> Union[pd.DataFrame, None]:
    """
    Retrieves a list of new everyone ids from BigQuery and returns the results as a pandas DataFrame.

    This function retrieves a list of new everyone ids from a BigQuery table by querying the table for all ids
    that associate with more than 800,000 users or members. The function logs a message indicating the number
    of new ids that were retrieved, and returns the results as a pandas DataFrame.

    Args:
        None

    Returns:
        pd.DataFrame: A pandas DataFrame containing the results of the list of new everyone ids.

    """
    logger = logging.getLogger("primary_logger")
    ci = SingletonConfig()
    project_id = ci.project_id
    table = table_id.split("_")[1]
    qry_new_everyone_ids = f"""
    select {table}_id as id
    from {project_id}.el_okta.{table_id}
    group by {table}_id
    having count({table}_id) > 800000;
    """
    logger.info(f"Getting new everyone ids")
    df_result = query_bigquery_as_dataframe(
        qry_new_everyone_ids, "CRU_DATA_WAREHOUSE_ELT_PROD"
    )
    if df_result is not None:
        if not df_result.empty:
            df_result_list = df_result["id"].tolist()
            logger.info(
                f"Retrieved {len(df_result_list)} new everyone_ids from previous data pull: {df_result_list}."
            )
            return df_result
        else:
            logger.info(f"No new everyone_ids retrieved from previous data pull.")
            return pd.DataFrame()
    else:
        logger.error(
            f"Retrieve new everyone ids from bigquery error: no data returned."
        )
        return None


def get_current_everyone_ids_bigquery(table_id: str) -> Union[pd.DataFrame, None]:
    """
    Retrieves the list of current everyone ids from a BigQuery table.

    This function retrieves the list of current everyone ids from a BigQuery table by querying the table for all ids.
    The function logs a message indicating the number of ids that were retrieved, and returns a pandas DataFrame
    containing the results.

    Args:
        table_id (str): The ID of the BigQuery table to query.

    Returns:
        pd.DataFrame: A pandas DataFrame containing the results of the query.

    """
    logger = logging.getLogger("primary_logger")
    ci = SingletonConfig()
    project_id = ci.project_id
    qry_current_everyone_ids = f"""
    select * from {project_id}.el_okta.{table_id};
    """
    logger.info(f"Getting the list of current everyone ids")
    df_result = query_bigquery_as_dataframe(
        qry_current_everyone_ids, "CRU_DATA_WAREHOUSE_ELT_PROD"
    )
    if df_result is not None and not df_result.empty:
        df_result_list = df_result["id"].tolist()
        logger.info(
            f"Retrieved current everyone ids from bigquery. Total count: {len(df_result_list)}."
        )
        return df_result
    else:
        logger.error(
            f"Retrieve current everyone ids from bigquery error: no data returned."
        )
        return None


def write_to_csv(df: pd.DataFrame, file_name: str) -> None:
    """
    Writes a pandas DataFrame to a CSV file.

    This function writes a pandas DataFrame to a CSV file with the specified file name. The function logs a message
    indicating the file name and the number of rows that were written to the file.

    Args:
        df (pd.DataFrame): The pandas DataFrame to write to the CSV file.
        file_name (str): The name of the CSV file to write.

    Returns:
        None

    """
    logger = logging.getLogger("primary_logger")
    ci = SingletonConfig()
    folder_path = ci.log_path
    file_path = os.path.join(folder_path, file_name)
    df.to_csv(f"{file_path}.csv", index=False, quoting=csv.QUOTE_MINIMAL, quotechar='"')
    logger.info("Created CSV file: " + file_name)


def get_downloaded_files() -> None:
    """
    Download data from BigQuery then write to csv files.

    This function is mainly used to download data from BigQuery bgiquery
    table then write to csv file. This functions is mainly used to
    download the finished "okta_apps" and "okta_groups" tables to resume
    the sync process when the process is interrupted.

    Args:
        None

    Returns:
        None
    """
    ci = SingletonConfig()
    project_id = ci.project_id
    dataset_id = ci.dataset_id
    df = download_from_bigquery_as_dataframe(
        project_id, dataset_id, "okta_apps", "CRU_DATA_WAREHOUSE_ELT_PROD"
    )
    if df is not None:
        write_to_csv(df, "okta_apps")
    df = download_from_bigquery_as_dataframe(
        project_id, dataset_id, "okta_groups", "CRU_DATA_WAREHOUSE_ELT_PROD"
    )
    if df is not None:
        write_to_csv(df, "okta_groups")


def upload_log() -> None:
    """
    Uploads the log file to a Google Cloud Storage bucket.

    This function uploads the log file to the specified Google Cloud Storage bucket with
    the specified name.

    Args:
        None

    Returns:
        None

    """
    logger = logging.getLogger("primary_logger")
    logger.info(f"Uploading log file to Google Cloud Storage")
    ci = SingletonConfig()
    folder_path = ci.log_path
    file_path = os.path.join(folder_path, "output.log")
    log_folder_name = os.path.basename(folder_path)
    blob_name = f"okta/log/output_{log_folder_name}.log"
    upload_to_gcs(file_path, "el_files", blob_name, "CRU_DATA_WAREHOUSE_SANDBOX")


def sync_data(endpoint: str) -> None:
    """
    Synchronizes apps/users/groups data from Okta to BigQuery.

    This function retrieving data from the specified API endpoint by calling
    the get_data() function. The function then matches the schema of the downloaded data to the schema of the BigQuery table,
    writes the data to a CSV file, and uploads the data to the BigQuery table.

    Args:
        endpoint (str): The API endpoint to synchronize data from (e.g. "apps", "users" or "groups").

    Returns:
        None

    """
    logger = logging.getLogger("primary_logger")
    ci = SingletonConfig()
    project_id = ci.project_id
    dataset_id = ci.dataset_id
    token = get_general_credentials("OKTA_TOKEN")
    url = f"https://signon.okta.com/api/v1/{endpoint}"
    headers = {
        "Authorization": f"{token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    params = {"limit": 200}
    logger.info(f"Starting to download okta_{endpoint}...")
    df = get_data(endpoint, url, headers, params)
    if endpoint == "users":
        params = {"limit": 200, "search": 'status eq "DEPROVISIONED"'}
        logger.info(f"Starting to download okta_{endpoint} deprovisioned...")
        df_deprovisioned = get_data(endpoint, url, headers, params)
        df = pd.concat([df, df_deprovisioned], axis=0, ignore_index=True)
        logger.info(f"Deprovisioned users added to okta_{endpoint}")
    table_id = f"okta_{endpoint}"
    schema_json = get_schema(table_id)
    if schema_json is None:
        logger.error(
            f"Schema for {table_id} not found. Skip uploading to BigQuery. Write to csv only."
        )
        df = df.drop_duplicates()
        write_to_csv(df, table_id)
        return
    df = match_schema(df, schema_json)
    df = df.drop_duplicates()
    write_to_csv(df, table_id)
    try:
        upload_dataframe_to_bigquery(
            project_id,
            dataset_id,
            table_id,
            "CRU_DATA_WAREHOUSE_ELT_PROD",
            df,
            schema_json,
        )
    except Exception as e:
        logger.exception(f"Upload {table_id} to BigQuery failed: str(e)")
        pass


def sync_all_users(endpoint: str) -> None:
    """
    Synchronizes user data from Okta to Google BigQuery.

    This function retrieves all the apps/groups ids from the downloaded data, filters out any excluded ids. The function
    then download all the users data by calling get_all_users() function to download all users data fro all the app/group ids.
    Then it matches the schema of the downloaded data to the schema of the BigQuery table, writes the data to a CSV file,
    and uploads the data to the BigQuery table. Finally, the function updates the excluded user list in BigQuery.

    Args:
        endpoint (str): The endpoint to synchronize user data for (e.g. "group_members" or "app_users").

    Returns:
        None

    """
    logger = logging.getLogger("primary_logger")
    table_id = f"okta_{endpoint}"
    logger.info(f"Starting to download {table_id}...")
    schema_json = get_schema(table_id)
    if schema_json is None:
        logger.error(f"Schema for {table_id} not found. Sync group members failed.")
        return
    columns = [field["name"] for field in schema_json]  # type: ignore
    # Get all the group ids or app ids from the newly downloaded data
    ci = SingletonConfig()
    project_id = ci.project_id
    dataset_id = ci.dataset_id
    folder_path = ci.log_path
    file_path = os.path.join(folder_path, f"okta_{endpoint.split('_')[0]}s.csv")
    df_all = pd.read_csv(file_path)
    # Get the existing excluded everyone group ids or app ids
    df_everyone = get_current_everyone_ids_bigquery(
        f"okta_everyone_{endpoint.split('_')[0]}_ids"
    )
    # Excute query to get the new excluded everyone ids
    df_everyone_new = get_new_everyone_ids_bigquery(table_id)
    if df_everyone is None or df_everyone_new is None:
        logger.error(f"Excluded everyone ids not found.")
        return
    # Combine the existing and new excluded everyone group ids
    df_everyone = pd.concat([df_everyone, df_everyone_new]).drop_duplicates()
    list_all = df_all["id"].to_list()
    list_everyone = df_everyone["id"].to_list()
    ids = list(set(list_all) - set(list_everyone))
    ids.sort()
    token = get_general_credentials("OKTA_TOKEN")
    headers = {
        "Authorization": f"{token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    params = {"limit": "1000"} if endpoint == "group_members" else {"limit": "500"}
    # ids = ids[:2]  # for testing
    df = get_all_users(f"{endpoint.split('_')[0]}s", headers, params, ids, columns)
    df = match_schema(df, schema_json)
    df = df.drop_duplicates()
    write_to_csv(df, table_id)
    try:
        upload_dataframe_to_bigquery(
            project_id,
            dataset_id,
            table_id,
            "CRU_DATA_WAREHOUSE_ELT_PROD",
            df,
            schema_json,
        )
    except Exception as e:
        logger.exception(f"Upload {table_id} to BigQuery failed: str(e)")
        pass
    # Update the excluded everyone ids
    try:
        upload_dataframe_to_bigquery(
            project_id,
            dataset_id,
            f"okta_everyone_{endpoint.split('_')[0]}_ids",
            "CRU_DATA_WAREHOUSE_ELT_PROD",
            df_everyone,
        )
    except Exception as e:
        logger.exception(
            f"Upload okta_everyone_{endpoint.split('_')[0]}_ids to BigQuery failed: str(e)"
        )
        pass


def trigger_sync():
    """
    Main entry point for Cloud Run Jobs.
    This function will be called when the job is triggered.
    """
    logger = logging.getLogger("primary_logger")
    logger.info("Starting Okta data synchronization job")

    try:
        sync_data("apps")
        sync_data("users")
        sync_data("groups")
        sync_all_users("group_members")
        sync_all_users("app_users")
        replace_dataset_bigquery()
        # dbt_run("10206", "85521", "DBT_TOKEN")
        # upload_log()
        logger.info("Okta data synchronization job completed successfully")
    except Exception as e:
        logger.exception(f"Okta sync job failed: {str(e)}")
        raise


def main():
    """
    Legacy main function for backwards compatibility.
    """
    setup_logging()
    trigger_sync()


if __name__ == "__main__":
    setup_logging()
    trigger_sync()
