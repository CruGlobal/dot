import os
import logging
import requests

logger = logging.getLogger("primary_logger")

# Our account-specific dbt Cloud host (cloud.getdbt.com is retired). Set DBT_BASE_URL to
# override; the shared python package and dse-portal read the same variable.
DEFAULT_DBT_BASE_URL = "https://kw745.us1.dbt.com/api/v2"


def dbt_api_base() -> str:
    """The dbt Cloud API v2 base URL, without a trailing slash."""
    return (os.environ.get("DBT_BASE_URL") or DEFAULT_DBT_BASE_URL).rstrip("/")


class DbtClient:

    def __init__(self, access_token: str, account_id: str):
        self.access_token = access_token
        self.account_id = account_id
        self.headers = {
            "Authorization": f"Bearer {self.access_token}",
        }
        self.base_url = dbt_api_base()
        self.account_url = f"{self.base_url}/accounts/{account_id}"

    def _request(self, url, data=None, params=None, method="GET"):
        request_details = {"headers": self.headers}
        if data:
            request_details["data"] = data
        if params:
            request_details["params"] = params

        try:
            response = requests.request(method, url, **request_details)
            if response.ok:
                return response.json()
        except Exception as e:
            logger.exception(f"Error in making request to {url}: {e}")
            raise

    def trigger_job(self, job_id, cause="Triggered by Google Cloud Function"):
        logger.info(f"Triggering dbt job {job_id} on account {self.account_id}")
        response = self._request(
            f"{self.account_url}/jobs/{job_id}/run/",
            data={"cause": cause},
            method="POST",
        )
        return response
