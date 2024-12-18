import pandas as pd
from typing import Optional, Dict, Union, List
from google.cloud import bigquery
import google.auth
import logging

logger = logging.getLogger("primary_logger")


class BigQueryClient:
    """
    BigQueryClient is a class for interacting with the BigQuery API.
    Uses the default service account running the Google Cloud Function.

    Attributes:
        conn (bigquery.Client): The BigQuery client object
    """

    def __init__(self) -> None:
        self.credentials, self.project = google.auth.default()
        self.conn = bigquery.Client(credentials=self.credentials, project=self.project)

    @property
    def email(self):
        return self.credentials.service_account_email

    def upload_from_dataframe(
        self,
        df: pd.DataFrame,
        dataset: str,
        table: str,
        upload_type: str,
        schema: Optional[Union[List[List], Dict[str, str]]] = None,
    ):
        """Upload a pandas dataframe to a table in BigQuery
        Args:
            df: The dataframe to load
            dataset: The name of the dataset in BigQuery to upload to
            table: The name of the table to write to
            upload_type: Whether to append to or replace the data. Choices are 'overwrite' and 'append'
            schema: The optional schema of the table to be loaded
        """
        try:
            dataset_ref = bigquery.DatasetReference(self.project, dataset)
            table_ref = dataset_ref.table(table)
            job_config = bigquery.LoadJobConfig()

            if upload_type == "overwrite":
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
            else:
                job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            job_config.autodetect = True  # infer the schema
            if schema:
                job_config.autodetect = True
                job_config.schema = self._format_schema(schema)
            job = self.conn.load_table_from_dataframe(
                df, table_ref, job_config=job_config
            )
            job.result()
            logger.info(f"Dataframe uploaded to BigQuery {dataset}.{table}")
        except Exception as e:
            logger.exception(
                f"An error occurred when attempting to upload to BigQuery: {str(e)}"
            )
            raise

    def _format_schema(
        self, schema: Union[List[List[str]], Dict[str, List[str]]]
    ) -> List[bigquery.SchemaField]:
        """Helper function to format the schema appropriately for BigQuery

        Args:
            schema: The representation inputted as either a list of lists (for backwards compatibility) or preferably JSON

        Returns: The formatted schema

        """
        formatted_schema = []
        try:
            for item in schema:
                if isinstance(item, list):
                    formatted_schema.append(bigquery.SchemaField(*item))
                elif isinstance(item, dict):
                    formatted_schema.append(bigquery.SchemaField.from_api_repr(item))
                else:
                    logger.exception(
                        "Format of inputted schema is incorrect, this should preferably be a JSON representation or a List of Lists. For additional information and examples, visit https://cloud.google.com/bigquery/docs/schemas#specifying_a_json_schema_file"
                    )
                    raise
        except Exception as e:
            logger.exception(
                f"Error in preparing the inputted schema to the approrpriate BigQuery format: {str(e)}"
            )
            raise
        else:
            return formatted_schema
