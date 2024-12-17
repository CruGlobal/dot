import os
import logging
import requests

logger = logging.getLogger("primary_logger")


class DbtClient:

    def __init__(self, access_token: str, account_id: str):
        self.access_token = access_token
        self.account_id = account_id
        self.headers = {"Authorization": f"Bearer {access_token}"}
        self.account_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}"
        self.base_url = "https://cloud.getdbt.com/api/v2"

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

    def trigger_job(self, job_id):
        logger.info(f"Triggering dbt job {job_id} on account {self.account_id}")

        response = self._request(
            f"{self.account_url}/jobs/{job_id}/run/",
            data={"cause": f"Triggered by Google Cloud Function"},
            method="POST",
        )
        return response
