import time
import logging
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger("primary_logger")


class ExitCodeException(Exception):
    """
    ExitCodeException is a custom exception class for raising exceptions with exit codes.
    """

    def __init__(self, message, exit_code):
        super().__init__(message)
        self.message = message
        self.exit_code = exit_code


class FivetranClient:
    """
    FivetranClient is a class for interacting with the Fivetran API.

    Attributes:
        api_key (str): The Fivetran API key
        api_secret (str): The Fivetran API secret
        auth (HTTPBasicAuth): The authentication object used for requests
    """

    EXIT_CODE_INVALID_CREDENTIALS = 200
    EXIT_CODE_BAD_REQUEST = 201
    EXIT_CODE_SYNC_REFRESH_ERROR = 202
    EXIT_CODE_SYNC_ALREADY_RUNNING = 203
    EXIT_CODE_SYNC_INVALID_SOURCE_ID = 204
    EXIT_CODE_SYNC_INVALID_POKE_INTERVAL = 205
    EXIT_CODE_INVALID_INPUT = 206
    EXIT_CODE_UNKNOWN_ERROR = 249

    def __init__(self, api_key: str, api_secret: str) -> None:
        self.api_key = api_key
        self.api_secret = api_secret
        self.auth = HTTPBasicAuth(self.api_key, self.api_secret)
        logger.debug("FivetranClient initialized")

    def _request(
        self, endpoint: str, method: str = "GET", payload: dict = None
    ) -> dict:
        """
        Internal method to send a request to the Fivetran API.

        Parameters:
            endpoint (str): The endpoint of the Fivetran API
            method (str): The HTTP method of the request. Defaults to "GET".
            payload (dict): The payload of the request. Defaults to None.

        Raises:
            ExitCodeException: If the response status code is 401, 400, or others.

        Returns:
            dict: The JSON response from the Fivetran API
        """

        url = f"https://api.fivetran.com/v1/{endpoint}"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json;version=2",
        }

        try:
            if payload:
                resp = requests.request(
                    method, url, headers=headers, json=payload, auth=self.auth
                )
            else:
                resp = requests.request(method, url, headers=headers, auth=self.auth)

            resp.raise_for_status()
            return resp.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"Error: {e.response.status_code} - {e.response.text}")
            if e.response.status_code == 401:
                raise ExitCodeException(
                    f"Authentication failed: {e.response.json().get('message')}",
                    self.EXIT_CODE_INVALID_CREDENTIALS,
                )
            elif e.response.json().get("code") == "NotFound_Integration":
                raise ExitCodeException(
                    f"Invalid source ID: {e.response.json().get('message')}",
                    self.EXIT_CODE_SYNC_INVALID_SOURCE_ID,
                )
            elif e.response.status_code == 400:
                raise ExitCodeException(
                    f"Bad request: {e.response.json().get('message')}",
                    self.EXIT_CODE_BAD_REQUEST,
                )
            else:
                raise ExitCodeException(
                    f"Unknown error: {e.response.json()}", self.EXIT_CODE_UNKNOWN_ERROR
                )

    def trigger_sync(
        self,
        connector_id: str,
        force: bool = True,
        wait_for_completion: bool = False,
        poke_interval: int = 30,
    ):
        """
        Trigger a sync on a specific connector.

        Parameters:
            connector_id (str): The ID of the connector
            force (bool): Whether to force the sync. Defaults to True.
            wait_for_completion (bool): Whether to wait for the sync to complete. Defaults to False.
            poke_interval (int): Interval in seconds to check for sync completion. Defaults to 30.

        Raises:
            ExitCodeException: If an error occurs while triggering sync.
        """

        if wait_for_completion:
            logger.info(
                "Getting connection details for last successful and failed syncs..."
            )
            prev_success, prev_failure = self._get_latest_success_and_failure(
                connector_id
            )
            logger.info(
                f"The last success was {prev_success or 'never'} and the last failure was {prev_failure or 'never'}"
            )

        try:
            self._request(
                endpoint=f"connectors/{connector_id}/force",
                method="POST",
                payload={"force": force},
            )
        except ExitCodeException as e:
            logger.error(f"Error triggering sync: {e}")
            raise ExitCodeException(f"Error triggering sync: {e}", e.exit_code) from e
        else:
            logger.info("Sync triggered successfully")
            if wait_for_completion:
                new_success, new_failure = prev_success, prev_failure
                while prev_success == new_success and prev_failure == new_failure:
                    logger.info("Waiting for sync to complete...")
                    time.sleep(poke_interval)
                    new_success, new_failure = self._get_latest_success_and_failure(
                        connector_id
                    )

                logger.info("Sync completed")
                logger.info("Checking for new failure")
                if (prev_failure and new_failure != prev_failure) or (
                    not prev_failure and new_failure
                ):
                    logger.error(f"Sync failed at {new_failure}")
                    raise ExitCodeException(
                        f"Sync failed at {new_failure}",
                        self.EXIT_CODE_SYNC_REFRESH_ERROR,
                    )
                else:
                    logger.info("No new failure detected")

    def determine_sync_status(self, connector_id: str) -> str:
        """
        Determine the sync status of a specific connector.

        Parameters:
            connector_id (str): The ID of the connector.

        Raises:
            ExitCodeException: If an error occurs while getting the sync status.

        Returns:
            str: The sync state of the connector.
        """

        try:
            response = self._request(
                endpoint=f"connectors/{connector_id}", method="GET"
            )
            return response.get("data", {}).get("status", {}).get("sync_state")
        except ExitCodeException as e:
            logger.error(f"Error determining sync status: {e}")
            raise ExitCodeException(
                f"Error determining sync status: {e}", e.exit_code
            ) from e

    def get_connector_details(self, connector_id: str) -> dict:
        """
        Get the details of a specific connector.

        Parameters:
            connector_id (str): The ID of the connector.

        Raises:
            ExitCodeException: If an error occurs while getting the connector details.

        Returns:
            dict: The details of the connector.
        """

        try:
            response = self._request(
                endpoint=f"connectors/{connector_id}", method="GET"
            )
            return response.get("data", {})
        except ExitCodeException as e:
            logger.error(f"Error getting connector details: {e}")
            raise ExitCodeException(
                f"Error getting connector details: {e}", e.exit_code
            ) from e

    def _get_latest_success_and_failure(self, connector_id: str) -> tuple:
        """
        Internal method to get the details of the latest successful and failed syncs for a specific connector.

        Parameters:
            connector_id (str): The ID of the connector.

        Returns:
            tuple: A tuple containing the timestamp of the latest successful sync and the latest failed sync.
        """
        current_details = self.get_connector_details(connector_id)
        success = current_details.get("succeeded_at")
        failure = current_details.get("failed_at")
        return success, failure

    def update_connector(
        self,
        connector_id: str,
        schedule_type: str = None,
        paused: bool = None,
        historical_sync: bool = None,
        additional_details: dict = None,
    ) -> None:
        """
        Update the settings of a specific connector.

        Parameters:
            connector_id (str): The ID of the connector.
            schedule_type (str): The type of schedule for the connector. Defaults to None.
            paused (bool): Whether the connector should be paused. Defaults to None.
            historical_sync (bool): Whether to trigger a historical sync. Defaults to None.
            additional_details (dict): Additional details to update. Defaults to None.

        Raises:
            ExitCodeException: If an error occurs while updating the connector.
        """

        payload = {
            "schedule_type": schedule_type,
            "paused": paused,
            "historical_sync": historical_sync,
        }
        if additional_details:
            payload |= additional_details

        if payload := {k: v for k, v in payload.items() if v is not None}:
            endpoint = f"connectors/{connector_id}"
            try:
                self._request(endpoint, method="PATCH", payload=payload)
            except ExitCodeException as e:
                logger.error(f"Error updating connector: {e}")
                raise ExitCodeException(
                    f"Error updating connector: {e}", e.exit_code
                ) from e
            else:
                logger.info("Connector updated successfully")
        else:
            logger.error("No updates to connector were provided")
            raise ExitCodeException(
                "No updates to connector were provided", self.EXIT_CODE_BAD_REQUEST
            )

    def connect(self) -> int:
        """
        Verifies connection to Fivetran API.

        Returns:
            int: 0 if connection is valid, 1 if connection is invalid
        """

        try:
            self._request(endpoint="users", method="GET")
            logger.info("Connection Validated")
            return 0
        except Exception as e:
            logger.error(f"Error connecting to Fivetran: {e}")
            return 1
