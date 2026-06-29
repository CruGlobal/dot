import logging
import requests

logger = logging.getLogger("primary_logger")

DBT_API_BASE = "https://cloud.getdbt.com/api/v2"


class DbtReadClient:
    """Read-only dbt Cloud API client for the classifier: fetch a failed run's
    metadata (trigger + run steps) and its run_results.json artifact."""

    def __init__(self, access_token: str, account_id: str):
        self.account_id = account_id
        # dbt Cloud service tokens authenticate as "Token <key>" (POC-validated for
        # these read endpoints).
        self.headers = {"Authorization": f"Token {access_token}"}
        self.account_url = f"{DBT_API_BASE}/accounts/{account_id}"

    def get_run(self, run_id: str) -> dict:
        """GET the run including its trigger (for the loop-guard cause) and run_steps.
        Returns the `data` object; raises on a non-2xx response."""
        resp = requests.get(
            f"{self.account_url}/runs/{run_id}/",
            params={"include_related": '["trigger","run_steps"]'},
            headers=self.headers,
        )
        resp.raise_for_status()
        return resp.json().get("data", {})

    def get_run_results(self, run_id: str) -> list:
        """GET the run_results.json artifact. Returns the results[] list (possibly
        empty); raises on a non-2xx response (e.g. artifact not produced)."""
        resp = requests.get(
            f"{self.account_url}/runs/{run_id}/artifacts/run_results.json",
            headers=self.headers,
        )
        resp.raise_for_status()
        return resp.json().get("results", [])
