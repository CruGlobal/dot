import os
import json
import base64
import logging
import time
import requests
from typing import List, Dict, Any, Optional, Union
import pandas as pd
import google.auth
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField
from google.cloud import storage


def get_general_credentials(secret_name: str) -> Union[str, None]:
    """
    Retrieves a secret value from environment variables.

    This function retrieves a secret value from environment variables
    following the pattern used by other Cloud Run functions in this project.

    Args:
        secret_name (str): The name of the environment variable containing
        the secret value.

    Returns:
        str or None: The secret value as a string or None if failed to
        retrieve secret value.
    """
    logger = logging.getLogger("primary_logger")
    secret_value = os.environ.get(secret_name)
    if secret_value is None:
        logger.error(f"Failed to get secret value with {secret_name}")
        return None
    
    # Strip any whitespace that might be present
    secret_value = secret_value.strip()
    logger.info(f"Successfully retrieved secret: {secret_name}")
    return secret_value


def get_google_credentials(secret_name: str) -> Union[Credentials, None]:
    """
    Retrieves Google Cloud credentials using Application Default Credentials.

    This function uses the default service account credentials available in the
    Cloud Run environment, similar to how process-geography works.

    Args:
        secret_name (str): Not used, kept for compatibility with existing code.

    Returns:
        A `Credentials` object or None: Retrieved Google Cloud credentials.
        or None if failed to retrieve credentials.
    """
    logger = logging.getLogger("primary_logger")
    try:
        credentials, _ = google.auth.default(
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive", 
                "https://www.googleapis.com/auth/bigquery",
            ]
        )
        return credentials
    except Exception as e:
        logger.exception(f"Get Google Cloud credentials error with {str(e)}")
        return None


def get_request(
    url: str,
    headers: Dict[str, str],
    params: Dict[Any, Any],
    max_retries: int = 5,
) -> Union[requests.Response, None]:
    """
    Sends an HTTP GET request to the specified URL with the specified headers.

    This function sends an HTTP GET request to the specified URL with the
    specified headers. If the response is not a valid JSON object, the
    function will retry the request up to `max_retries` times. If the response
    is a 429 error, the function will retry the request with an increasing
    delay between retries.

    Args:
        url (str): The URL to send the request to.
        headers (Dict[str, str]): A dictionary of headers to include in the
        GET request.
        params (Dict[Any, Any]): A dictionary of parameters to include in the
        GET request.
        max_retries (int): The maximum number of times to retry the request
        if the response is not a valid JSON object. Defaults to 5.

    Returns:
        requests.Response or None: The response object if the request is
        successful and returns valid JSON, or None if the request fails after
        the maximum number of retries.

    Raises:
        requests.exceptions.HTTPError: If the GET request encounters an HTTP
        error (other than 429).
        requests.exceptions.Timeout: If the GET request times out.
        requests.exceptions.ConnectionError: If there is a connection error
        during the GET request.
        requests.exceptions.RequestException: If there is a general request
        error.
    """
    logger = logging.getLogger("primary_logger")
    retries = 0
    while retries <= max_retries:
        try:
            r = requests.get(url, headers=headers, params=params, timeout=60)
            r.raise_for_status()
            try:
                _ = r.json()
                return r
            except ValueError as e:
                # In case of invalid JSON response, retry the request
                retries += 1
                logger.warning(
                    f"Invalid JSON response: {str(e)}. Retry in 5 minutes..."
                )
                time.sleep(300)
        except requests.exceptions.HTTPError as err:
            if err.response.status_code == 429:  # Handle 429 error
                logger.warning("API rate limit exceeded. Retry in 1 second...")
                delay = 1  # Initial delay is 1 second
                while True:
                    time.sleep(delay)  # Wait for the delay period
                    try:
                        r = requests.get(
                            url, headers=headers, params=params, timeout=60
                        )  # Retry the same URL
                        r.raise_for_status()
                        try:
                            _ = r.json()
                            return r
                        except ValueError as e:
                            retries += 1
                            logger.warning(
                                f"Invalid JSON response: {str(e)}. Retry in 5 minutes..."
                            )
                            time.sleep(300)
                    except requests.exceptions.HTTPError as err:
                        if err.response.status_code == 429:  # Handle 429 error
                            logger.warning(
                                f"API rate limit exceeded. Retry in {delay*2} seconds..."
                            )
                            delay *= 2  # Double the delay period
                            continue  # Retry the same URL
                        else:
                            logger.warning(
                                f"HTTP error: {str(err)}. Retry in {delay*2} seconds..."
                            )
                            delay *= 2
                            continue
                    break  # Break out of the retry loop if the request is successful
            else:
                logger.warning(f"HTTP error: {str(err)}. Retry in 3 minutes...")
                retries += 1
                time.sleep(180)
        except requests.exceptions.Timeout as e:
            logger.warning(f"Request timed out: {str(e)}. Retry in 1 minutes...")
            retries += 1
            time.sleep(60)
        except requests.exceptions.ConnectionError as e:
            logger.warning(
                f"Get request connection error: {str(e)}. Retry in 1 minutes..."
            )
            retries += 1
            time.sleep(60)
        except requests.exceptions.RequestException as e:
            logger.warning(
                f"Get general request error: {str(e)}. Retry in 5 minutes..."
            )
            retries += 1
            time.sleep(300)
    logger.error(f"Get request failed after {max_retries + 1} attempts.")
    return None


def validate_upload_dataframe_to_bigquery_arguments(
    project_id: str,
    dataset_id: str,
    table_id: str,
    secret_name: str,
    df: pd.DataFrame,
):
    """
    Validate the arguments needed for upload_bigquery_dataframe().

    Args:
        project_id (str): The Google Cloud project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        secret_name (str): The name of the environment variable
        used for Google Cloud authentication.
        df (pd.DataFrame): The pandas DataFrame containing the data to
        upload.

    Raises:
        AssertionError: If any of the arguments are None or if df is not
        a pandas DataFrame.
    """
    assert project_id, "project_id must not be None or empty"
    assert dataset_id, "dataset_id must not be None or empty"
    assert table_id, "table_id must not be None or empty"
    assert secret_name, "secret_name must not be None or empty"
    assert isinstance(df, pd.DataFrame), "df must be a pandas DataFrame"


def get_job_config(schema_json, write_disposition, job_config_override):
    """
    Get the BigQuery LoadJobConfig based on provided arguments.

    Args:
        schema_json (Dict[Any, Any]): A dictionary representing the schema
        of the BigQuery table.
        write_disposition (str): Write disposition for the load job. Default
        is "WRITE_TRUNCATE", other options are "WRITE_APPEND" and
        "WRITE_EMPTY".
        job_config_override (bigquery.LoadJobConfig): An optional
        LoadJobConfig instance. If provided, this config will be used and
        other parameters will be ignored.

    Returns:
        bigquery.LoadJobConfig: The load job configuration for uploading
        data to BigQuery.
    """
    if job_config_override:
        return job_config_override
    if schema_json:
        schema = [SchemaField.from_api_repr(field) for field in schema_json]
        return bigquery.LoadJobConfig(
            schema=schema,
            create_disposition="CREATE_IF_NEEDED",
            write_disposition=write_disposition,
        )
    return bigquery.LoadJobConfig(
        autodetect=True,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition=write_disposition,
    )


def upload_dataframe_to_bigquery(
    project_id: str,
    dataset_id: str,
    table_id: str,
    secret_name: str,
    df: pd.DataFrame,
    schema_json: Optional[Dict[Any, Any]] = None,
    job_config_override: Optional[bigquery.LoadJobConfig] = None,
    write_disposition: Optional[str] = "WRITE_TRUNCATE",
) -> None:
    """
    Uploads data from a pandas DataFrame to a BigQuery table.

    This function validates the provided arguments, retrieves the Google Cloud
    credentials, prepares the load job configuration and then starts the
    upload process.

    Args:
        project_id (str): The Google Cloud project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        secret_name (str): The name of the secret in Google Cloud Secret Manager
        for Google Cloud authentication.
        df (pd.DataFrame): The pandas DataFrame containing the data to upload.
        write_disposition (str, optional): Write disposition for the load job.
        Default is "WRITE_TRUNCATE". Other options are "WRITE_APPEND" and
        "WRITE_EMPTY".
        schema_json (Dict[Any, Any], optional): A dictionary representing the
        schema of the BigQuery table.
        job_config_override (bigquery.LoadJobConfig, optional): An optional
        LoadJobConfig instance. If provided, this config will be used and
        other parameters will be ignored.

    Raises:
        ValueError: If Google Cloud credentials are invalid or absent.
        Any exception raised during the upload process will be re-raised after
        being logged.
    """
    validate_upload_dataframe_to_bigquery_arguments(
        project_id, dataset_id, table_id, secret_name, df
    )

    logger = logging.getLogger("primary_logger")
    credentials = get_google_credentials(secret_name)
    if credentials is None:
        logger.error("Upload to BigQuery error with authorization error")
        raise ValueError("Invalid Google Cloud credentials")

    client = bigquery.Client(credentials=credentials)
    table_id_full = f"{project_id}.{dataset_id}.{table_id}"
    job_config = get_job_config(schema_json, write_disposition, job_config_override)

    logger.info(f"Starting to upload data to {table_id_full}")
    try:
        job = client.load_table_from_dataframe(df, table_id_full, job_config=job_config)
        job.result()
        logger.info(f"Uploaded data to {table_id_full}")
    except Exception as e:
        logger.exception(f"Upload to BigQuery error: {str(e)}")
        raise


def download_from_bigquery_as_dataframe(
    project_id: str, dataset_id: str, table_id: str, secret_name: str
) -> Union[pd.DataFrame, None]:
    """
    Download data from a BigQuery table to a pandas DataFrame.

    This function downloads data from the specified BigQuery table.
    The function logs a message indicating whether the download succeeded or
    failed.

    Args:
        project_id (str): The Google Cloud project ID.
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        secret_name (str): The name of the secret in Google Cloud Secret Manager
        for Google Cloud authentication.

    Returns:
        Union[pd.DataFrame, None]: The pandas DataFrame containing the data
        downloaded from BigQuery. If the authorization or download fails,
        None is returned.
    """
    logger = logging.getLogger("primary_logger")
    credentials = get_google_credentials(secret_name)
    if credentials is None:
        logger.error("Download from BigQuery error with authorization error")
        return None
    client = bigquery.Client(credentials=credentials)
    table_id_full = f"{project_id}.{dataset_id}.{table_id}"
    logger.info(f"Starting to download data from BigQuery table: {table_id_full}")
    try:
        df = client.list_rows(table_id_full).to_dataframe()
        logger.info(f"Downloaded data from BigQuery table: {table_id_full}")
        return df
    except Exception as e:
        logger.exception(f"Download from BigQuery error: {str(e)}")
        return None


def query_bigquery_as_dataframe(
    query: str, secret_name: str
) -> Union[pd.DataFrame, None]:
    """
    Executes a SQL query on a BigQuery dataset and returns the results as
    a pandas DataFrame.

    This function executes a SQL query on a BigQuery dataset and returns
    the results as a pandas DataFrame. The function logs a message indicating
    the query that was executed, and logs a message indicating the number of
    rows that were returned by the query.

    Args:
        query (str): The SQL query to execute.
        secret_name (str): The name of the secret in Google Cloud Secret Manager
        for Google Cloud authentication.

    Returns:
        Union[pd.DataFrame, None]: A pandas DataFrame containing the results
        of the query. If the authorization or query fails, None is returned.
    """
    logger = logging.getLogger("primary_logger")
    credentials = get_google_credentials(secret_name)
    if credentials is None:
        logger.error("Query BigQuery error with authorization error")
        return None
    client = bigquery.Client(credentials=credentials)
    try:
        query_job = client.query(query)
        results = query_job.result()
        logger.info(f"Executed query.")
        return results.to_dataframe()
    except Exception as e:
        logger.exception(f"Query BigQuery error: {str(e)}")
        return None


def upload_to_gcs(
    file_path: str, bucket_name: str, blob_name: str, secret_name: str
) -> None:
    """
    Uploads the specified file to a Google Cloud Storage bucket.

    This function uploads a file to the specified Google Cloud Storage bucket
    with the specified name. The function logs a message indicating whether
    the upload succeeded or failed.

    Args:
        file_path: str: The path to the file to be uploaded.
        bucket_name (str): The name of the Google Cloud Storage bucket to
        upload the file to.
        blob_name (str): The name of the file to create in the bucket.
        secret_name (str): The name of the secret in Google Cloud Secret Manager
        for Google Cloud authentication.

    Returns:
        None
    """
    logger = logging.getLogger("primary_logger")
    credentials = get_google_credentials(secret_name)
    if credentials is None:
        logger.error(f"Failed to get Google Cloud credentials with {secret_name}")
        return None
    client = storage.Client(credentials=credentials)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    try:
        blob.upload_from_filename(file_path)
        logger.info(
            f"Uploaded file to Google Cloud Storage: gs://{bucket_name}/{blob_name}"
        )
    except Exception as e:
        logger.exception(f"Upload to Google Cloud Storage error: {str(e)}")


def get_dbt_job_list(account_id: str, secret_name: str) -> None:
    """
    Fetches a list of dbt jobs for a given account.

    This function retrieves the dbt jobs list from the dbt Cloud API for a
    given account. The function logs a message indicating whether the
    operation was successful or failed. This function is intended to be
    used for testing dbt connections and permissions.

    Args:
        account_id (str): The ID of the account in dbt Cloud.
        secret_name (str): The name of the secret in Google Cloud Secret Manager
        for dbt Cloud API token.

    Returns:
        None.
    """
    logger = logging.getLogger("primary_logger")
    token = get_general_credentials(secret_name)
    if token is None:
        logger.error(f"Failed to get dbt token with {secret_name}")
        return None
    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }
    url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/"
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        logger.info(f"Got dbt job list.")
    except Exception as e:
        logger.exception(f"List dbt job error: {str(e)}")


def trigger_dbt_job(account_id: str, job_id: str, token: str) -> Union[str, None]:
    """
    Triggers a dbt job.

    This function triggers a dbt job by sending a POST request to the
    specified URL with the specified headers and JSON payload. The function
    logs a message indicating whether the dbt job started successfully or
    failed, and returns the id of the dbt job run.

    Args:
        account_id (str): The ID of the account in dbt Cloud.
        job_id (str): The ID of the dbt job to be triggered.
        token (str): The dbt Cloud API token.

    Returns:
        str: The run_id of the dbt job run.
        None: If the dbt job failed to start.
    """
    logger = logging.getLogger("primary_logger")
    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }
    url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/jobs/{job_id}/run/"
    json = {"cause": "Triggered by python script API request"}
    try:
        r = requests.post(url, headers=headers, json=json)
        r.raise_for_status()
        logger.info(f"dbt job started successfully.")
        run_id = r.json()["data"]["id"]
        return run_id
    except Exception as e:
        logger.exception(f"dbt job failed to start: {str(e)}")
        return None


def get_dbt_run_status(run_id: str, token: str) -> int:
    """
    Retrieves the status of a dbt job run.

    This function retrieves the status of a dbt job run by sending a GET
    request to the specified URL with the specified headers and the dbt
    job run_id. The function logs a message indicating whether the dbt job
    status check succeeded or failed, and returns the status of the dbt
    job run.

    Args:
        run_id (str): The id of the dbt job run.
        token (str): The dbt Cloud API token.

    Returns:
        int: The status of the dbt job run.

    """
    logger = logging.getLogger("primary_logger")
    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/json",
    }
    url = f"https://cloud.getdbt.com/api/v2/accounts/10206/runs/{run_id}/"
    try:
        r = requests.get(url, headers=headers)
        r.raise_for_status()
        status = r.json()["data"]["status"]
    except Exception as e:
        logger.exception(f"dbt job status check failed: {e}")
        status = 0
    return status


def dbt_run(
    account_id: str, job_id: str, secret_name: str, max_retries: int = 3
) -> None:
    """
    Runs a dbt job and checks its status, with retry logic.

    This function runs a dbt job by calling the `trigger_dbt_job()` function
    and then checks the status of the job by calling `get_dbt_run_status()`
    function every 30 seconds until the job is completed successfully,
    failed, or cancelled. If the job fails, it will retry up to 'max_retries' times.
    The function logs messages indicating the status of the dbt job and its final outcome.

    Args:
        account_id (str): The ID of the account in dbt Cloud.
        job_id (str): The ID of the dbt job to be triggered.
        secret_name (str): The name of the secret in Google Cloud Secret Manager
        for dbt Cloud API token.
        max_retries (int): Maximum number of retries if the job fails. Default is 3.

    Returns:
        None
    """
    logger = logging.getLogger("primary_logger")
    logger.info(f"Starting dbt job...")
    token = get_general_credentials(secret_name)
    if token is None:
        logger.error(f"Failed to get dbt token with {secret_name}")
        return

    retries = 0
    while retries <= max_retries:
        run_id = trigger_dbt_job(account_id, job_id, token)
        if run_id is None:
            logger.error(
                f"dbt run failed to start. Retry {retries + 1} of {max_retries + 1}"
            )
            retries += 1
            continue

        while True:
            time.sleep(30)
            status = get_dbt_run_status(run_id, token)
            status_str = {
                0: "Status not available",
                1: "Queued",
                2: "Starting",
                3: "Running",
                10: "Success",
                20: "Failed",
                30: "Cancelled",
            }.get(status, "Unknown")

            logger.info(f"dbt job status: {status} - {status_str}")

            if status == 10:
                logger.info(f"dbt job run completed successfully.")
                return  # Exit the function on success
            elif status in (20, 30):  # Failed or Cancelled
                if retries < max_retries:
                    logger.warning(
                        f"dbt job {status_str}. Retrying... (Attempt {retries + 1} of {max_retries})"
                    )
                    retries += 1
                    break  # Break the inner loop to retry
                else:
                    logger.error(f"dbt job {status_str} after {max_retries} retries.")
                    return  # Exit the function after max retries

    logger.error(
        f"dbt job failed to complete successfully after {max_retries} retries."
    )
