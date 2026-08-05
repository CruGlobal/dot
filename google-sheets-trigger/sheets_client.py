"""
Google Sheets Client

Uses the Google Drive API to retrieve file metadata (specifically modifiedTime)
for Google Sheets files.
"""

from datetime import datetime
import logging

import google.auth
from googleapiclient.discovery import build


logger = logging.getLogger("primary_logger")


class SheetsClient:
    """
    Client for retrieving Google Sheets metadata via the Drive API.

    Uses Application Default Credentials (ADC) for authentication,
    which works automatically in Cloud Run/Cloud Functions.
    """

    def __init__(self):
        """
        Initialize the SheetsClient with Google Drive API credentials.

        Uses Application Default Credentials with drive.metadata.readonly scope
        to read file metadata without accessing file contents.
        """
        credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/drive.metadata.readonly"]
        )
        self.service = build("drive", "v3", credentials=credentials)
        logger.debug("SheetsClient initialized successfully")

    def get_modified_time(self, file_id: str) -> datetime:
        """
        Get the last modified time of a Google Sheet.

        Args:
            file_id: The Google Sheet file ID (from the URL:
                     docs.google.com/spreadsheets/d/{file_id}/edit)

        Returns:
            datetime: The last modified time as a timezone-aware datetime (UTC).

        Raises:
            googleapiclient.errors.HttpError: If the file cannot be accessed
                (e.g., not shared with the service account, or invalid ID).
        """
        file_metadata = self.service.files().get(
            fileId=file_id,
            fields="modifiedTime,name",
            supportsAllDrives=True
        ).execute()

        modified_time_str = file_metadata["modifiedTime"]
        # Google returns ISO format with Z suffix: "2024-01-15T14:30:00.000Z"
        # Convert to timezone-aware datetime
        modified_time = datetime.fromisoformat(
            modified_time_str.replace("Z", "+00:00")
        )

        logger.debug(
            f"Retrieved modifiedTime for file {file_id}: {modified_time.isoformat()}"
        )
        return modified_time
