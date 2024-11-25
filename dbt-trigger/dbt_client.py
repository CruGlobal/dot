import os
import logging
import requests

logger = logging.getLogger("primary_logger")


class DbtClient:

    def __init__(self, access_token: str, account_id: str):
        self.access_token = access_token
        self.account_id = account_id
        self.headers = {"Authorization": f"Bearer {access_token}"}
        self.account_url = f"https://cloud.getdbt.com/api/v2/accounts/{account_id}/"
        self.base_url = "https://cloud.getdbt.com/api/v2/"

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
        # logger.info(f"Succesfully triggered job {job_id}")
        # logger.info(f"Response from the API: \n{response}")
        return response

    def determine_run_status(self, run_id):
        run_details = self.get_run_details(run_id)
        run_id = run_details["data"]["id"]
        if run_details["data"]["is_complete"]:
            if run_details["data"]["is_error"]:
                logger.error(f"The run {run_id} failed.")
                return 20
            elif run_details["data"]["is_cancelled"]:
                logger.error(f"The run {run_id} was cancelled.")
                return 30
            else:
                logger.info(f"The run {run_id} was successful")
                return 10
        else:
            logger.info(f"The run {run_id} is not yet completed.")
            return 110

    def connect(self):
        try:
            response = requests.get(self.account_url, headers=self.headers)
            if response.status_code == 200:
                logger.info("Successfully connected to DBT")
                return 0
            else:
                logger.exception("Could not connect to DBT")
                return 1
        except Exception as e:
            logger.exception(f"Could not connect to DBT due to {e}")
            return 1

    def get_run_details(self, run_id):
        # logger.info(f"Checking run details for run {run_id}.")
        response = self._request(
            url=f"{self.account_url}/runs/{run_id}/",
            params={"include_related": "['run_steps','debug_logs']"},
        )
        # logger.info(f"Response from the API: \n{response}")
        return response

    def get_artifact_details(self, run_id):
        logger.info(f"Grabbing artifact details for run {run_id}")
        return self._request(f"{self.account_url}/runs/{run_id}/artifacts/")

    def download_artifact(self, run_id, artifact_name, destination_folder):
        get_artifact_details_url = (
            f"{self.account_url}/runs/{run_id}/artifacts/{artifact_name}"
        )
        artifact_file_name = os.path.basename(artifact_name)
        artifact_folder = os.path.dirname(artifact_name)

        destination_fullpath = os.path.join(destination_folder, artifact_folder)
        os.makedirs(destination_fullpath, exist_ok=True)

        filename = os.path.join(destination_fullpath, artifact_file_name)
        CHUNK_SIZE = 16 * 1024 * 1024

        try:
            with requests.get(
                get_artifact_details_url, headers=self.headers, stream=True
            ) as r:
                r.raise_for_status()
                with open(filename, "wb") as f:
                    for chunk in r.iter_content(chunk_size=CHUNK_SIZE):
                        f.write(chunk)
            logger.info(f"Successfully downloaded file {get_artifact_details_url}")
        except Exception as e:
            raise
