import os
import requests
import json
import logging
import sys
import pandas as pd
from pythonjsonlogger import jsonlogger
import base64
import time
import psutil
from datetime import datetime, timezone
from bigquery_client import BigQueryClient
from typing import Tuple, List, Dict, Any
from google.cloud import bigquery
from decimal import Decimal, getcontext
from collections import Counter

project_name = os.environ.get("BIGQUERY_PROJECT_NAME", None)
dataset_name = os.environ.get("BIGQUERY_DATASET_NAME", None)
client = BigQueryClient(project=project_name)

GET_CRU_LAST_LOAD_ORDERS= """
    select sync_timestamp 
    from `cru-data-warehouse-elt-prod.el_woocommerce_api.woo_api_orders`
    where rls_value = 'cru_woo'
    group by sync_timestamp
    order by sync_timestamp desc 
    limit 1
"""

GET_FL_LAST_LOAD_ORDERS= """
    select sync_timestamp 
    from `cru-data-warehouse-elt-prod.el_woocommerce_api.woo_api_orders`
    where rls_value = 'familylife_woo'
    group by sync_timestamp
    order by sync_timestamp desc 
    limit 1
"""

## SCRIPT UTILITIES -------------------------------------------------------------------------------------------------------------------------------------------
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

def log_memory_usage(checkpoint: str = ""):
    """
    Logs current memory usage for monitoring and debugging.

    Args:
        checkpoint (str): Description of where this is being called from
    """
    logger = logging.getLogger("primary_logger")
    try:
        memory = psutil.virtual_memory()
        process = psutil.Process()
        process_memory = process.memory_info()

        logger.info(
            f"Memory Usage {checkpoint}: "
            f"System: {memory.used / 1024**3:.2f}GB / {memory.total / 1024**3:.2f}GB "
            f"({memory.percent:.1f}%) | "
            f"Process: {process_memory.rss / 1024**3:.2f}GB"
        )
    except Exception as e:
        logger.warning(f"Failed to get memory usage: {str(e)}")

def handle_unhandled_exception(exc_type, exc_value, exc_traceback):
    """
    Handles unhandled exceptions by logging the exception details and sending an alert to the development team.

    This function is intended to be used as a custom excepthook function, which is called when an unhandled exception
    occurs in the application. The function logs the exception details to the primary logger, and sends an alert to
    the development team using a third-party service such as Datadog or PagerDuty.

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
    # Send an alert to the development team using a third-party service such as Datadog or PagerDuty
    # TODO: Add code to send an alert to the development team

def get_dtype_mapping() -> Dict[str, str]:
    """Return the mapping of schema types to pandas dtypes."""
    return {
        "string": "string",
        "integer": "Int64",
        "float": "float64",
        "object": "string",
        "date": "date",
        "datetime": "timestamp",
        "bool": "boolean",
        "numeric": "numeric",
        "bignumeric": "bignumeric",
        "timestamp": "timestamp"
    }

def create_dtype_dict(schema: list, dtype_mapping: Dict[str, str]) -> Dict[int, str]:
    """Create a dictionary mapping column indices to their data types."""
    return {i: dtype_mapping[col_type] for i, (_, col_type) in enumerate(schema)}

def load_to_dataframe(
    data: list,
    schema: list,
    skip_header_rows: int = 1,
) -> pd.DataFrame:
    """
    This function loads the dataframe
    """
    logger = logging.getLogger("primary_logger")
    try:
        dtype_mapping = get_dtype_mapping()
        dtypes = create_dtype_dict(schema, dtype_mapping)
        num_columns = len(schema)

        column_names = [col[0] for col in schema]        
        df = pd.DataFrame(data, columns=column_names)

        return df

    except Exception as e:
        logger.exception(f"Error occurred: {e}")
        raise

def get_last_load_date_time(obj):
    """
    This function gets the latest sync_timestamp value from the specified table
    """
    logger = logging.getLogger("primary_logger")
    query = obj
    client = bigquery.Client(project='cru-data-orchestration-poc')

    try:
        query_job = client.query(query)
        result = query_job.result()
        row = list(result)[0]
        last_update = row.sync_timestamp
        last_update = datetime.fromisoformat(str(last_update)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        return last_update

    except Exception as e:
        logger.info(f"Get last load date error: {str(e)}")

## BQ DATAFRAMES -------------------------------------------------------------------------------------------------------------------------------------------
def process_orders(list):
    """
    This function builds and uploads an orders dataframe
    """
    logger = logging.getLogger("primary_logger")
    table_name = "woo_api_orders"
    data = list
    schema = [
        ["store_wid", "integer"],
        ["rls_value", "string"],
        ["order_number", "integer"],
        ["sync_timestamp", "timestamp"],
        ["agent_email", "string"],
        ["agent_name", "string"],
        ["billing_address_1", "string"],
        ["billing_address_2", "string"],
        ["billing_city", "string"],
        ["billing_company", "string"],
        ["billing_country", "string"],
        ["billing_email", "string"],
        ["billing_first_name", "string"],
        ["billing_last_name", "string"],
        ["billing_phone", "string"],
        ["billing_postcode", "string"],
        ["billing_state", "string"],
        ["cart_hash", "string"],
        ["cart_tax", "bignumeric"],
        ["created_via", "string"],
        ["currency", "string"],
        ["custom_shipping_note", "string"],
        ["customer_id", "integer"],
        ["customer_ip_address", "string"],
        ["customer_note", "string"],
        ["customer_role", "string"],
        ["customer_user_agent", "string"],
        ["date_completed", "datetime"],
        ["date_created", "datetime"],
        ["date_modified", "datetime"],
        ["date_paid", "datetime"],
        ["date_shipped", "datetime"],
        ["discount_amount", "bignumeric"],
        ["discount_codes", "string"],
        ["discount_type", "string"],
        ["discount_description", "string"],
        ["discount_tax", "bignumeric"],
        ["discount_total", "bignumeric"],
        ["event_code", "string"],
        ["order_key", "string"],
        ["order_origin", "string"],
        ["order_type", "string"],
        ["ordered_by_email", "string"],
        ["ordered_by_name", "string"],
        ["ordered_by_phone", "string"],
        ["parent_id", "integer"],
        ["payment_method", "string"],
        ["payment_method_title", "string"],
        ["po_number", "string"],
        ["prices_include_tax", "bool"],
        ["radio_station", "string"],
        ["radio_station_description", "string"],
        ["salesforce_account", "string"],
        ["salesforce_id", "string"],
        ["shipped_method", "string"],
        ["shipping_address_1", "string"],
        ["shipping_address_2", "string"],
        ["shipping_city", "string"],
        ["shipping_company", "string"],
        ["shipping_country", "string"],
        ["shipping_first_name", "string"],
        ["shipping_last_name", "string"],
        ["shipping_method_id", "string"],
        ["shipping_method_title", "string"],
        ["shipping_postcode", "string"],
        ["shipping_state", "string"],
        ["shipping_tax", "bignumeric"],
        ["shipping_total", "bignumeric"],
        ["status", "string"],
        ["timestamp", "integer"],
        ["total", "bignumeric"],
        ["total_tax", "bignumeric"],
        ["transaction_id", "string"],
        ["version", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df['date_completed'] =  pd.DataFrame({'dates': pd.to_datetime(df['date_completed'])})
    df["date_created"] = pd.DataFrame({'dates': pd.to_datetime(df['date_created'])})
    df["date_modified"] = pd.DataFrame({'dates': pd.to_datetime(df['date_modified'])})
    df["date_paid"] = pd.DataFrame({'dates': pd.to_datetime(df['date_paid'])})
    df['date_shipped'] = df['date_shipped'].replace('0000-00-00 00:00:00', None)
    df['date_shipped'] = pd.to_datetime(df['date_shipped'], errors='coerce', utc=True)


    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_order_items(list):
    """
    This function builds and uploads an order_items dataframe
    """
    logger = logging.getLogger("primary_logger")
    table_name = "woo_api_order_items"
    data = list
    schema = [
        ["store_wid", "integer"],
        ["rls_value", "string"],
        ["order_item_id", "integer"],
        ["sync_timestamp", "timestamp"],
        ["date_created", "datetime"],
        ["order_key", "string"],
        ["order_number", "integer"],
        ["product_brand", "string"],
        ["product_component_cost", "bignumeric"],
        ["product_component_id", "integer"],
        ["product_component_msrp", "bignumeric"],
        ["product_component_regular_price", "bignumeric"],
        ["product_component_sku", "string"],
        ["product_cost", "bignumeric"],
        ["product_dept", "string"],
        ["product_discount", "bignumeric"],
        ["product_donor_premium", "bool"],
        ["product_exclude_discounting", "string"],
        ["product_free_shipping", "string"],
        ["product_gift_card", "string"],
        ["product_id", "integer"],
        ["product_impact", "string"],
        ["product_inactive", "string"],
        ["product_msrp", "bignumeric"],
        ["product_name", "string"],
        ["product_next_receipt_date", "string"],
        ["product_price", "bignumeric"],
        ["product_project", "string"],
        ["product_quantity", "integer"],
        ["product_regular_price", "bignumeric"],
        ["product_royalty", "string"],
        ["product_sku", "string"],
        ["product_subbrand", "string"],
        ["product_tax", "bignumeric"],
        ["product_total_manuals", "string"],
        ["product_weight", "bignumeric"],
        ["products_per_case", "integer"],
        ["bundled_by", "integer"],
        ["timestamp", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df['date_created'] =  pd.DataFrame({'dates': pd.to_datetime(df['date_created'])})    

    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_products(list):
    """
    This function builds and uploads a products dataframe
    """
    logger = logging.getLogger("primary_logger")
    table_name = "woo_api_products"
    data = list
    schema = [
        ["store_wid", "integer"],
        ["rls_value", "string"],
        ["sync_timestamp", "timestamp"],
        ["date_created", "timestamp"],
        ["date_modified", "timestamp"],
        ["id", "integer"],
        ["name", "string"],
        ["short_description", "string"],
        ["backorders_allowed", "bool"],
        ["downloadable", "bool"],
        ["virtual", "bool"],
        ["exclude_from_all_discounting", "bool"],
        ["free_shipping", "bool"],
        ["product_inactive", "bool"],
        ["gift_card", "bool"],
        ["donor_premium", "bool"],
        ["royalty", "bool"],
        ["next_receipt_date", "date"],
        ["brand", "string"],
        ["product_isbn", "string"],
        ["product_publisher", "string"],
        ["impact", "string"],
        ["product_language", "string"],
        ["sub_brand", "string"],
        ["status", "string"],
        ["alg_wc_cog_cost", "bignumeric"],
        ["fl_staff_price_field", "bignumeric"],
        ["msrp_price", "bignumeric"],
        ["price", "bignumeric"],
        ["regular_price", "bignumeric"],
        ["sku", "string"],
        ["weight", "bignumeric"],
        ["type", "string"],
        ["stock_quantity", "integer"],
        ["case_qty", "integer"],
        ["product_page_count", "integer"],
        ["total_manuals", "integer"],
        ["timestamp", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df['date_created'] =  pd.DataFrame({'dates': pd.to_datetime(df['date_created'])})
    df['date_modified'] =  pd.DataFrame({'dates': pd.to_datetime(df['date_modified'])})
    df["next_receipt_date"] = pd.to_datetime(df["next_receipt_date"]).dt.date

    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_product_bundles(list):
    """
    This function builds and uploads a product_bundles dataframe
    """
    logger = logging.getLogger("primary_logger")
    table_name = "woo_api_product_bundles"
    data = list
    schema = [
        ["store_wid", "integer"],
        ["rls_value", "string"],
        ["sync_timestamp", "timestamp"],
        ["id", "bignumeric"],
        ["bundled_item_id", "bignumeric"],
        ["product_id", "bignumeric"],
        ["quantity_default", "integer"],
        ["timestamp", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_product_categories(list):
    """
    This function builds and uploads a product_categories dataframe
    """
    logger = logging.getLogger("primary_logger")
    table_name = "woo_api_product_categories"
    data = list
    schema = [
        ["store_wid", "integer"],
        ["rls_value", "string"],
        ["sync_timestamp", "timestamp"],
        ["product_id", "integer"],
        ["id", "integer"],
        ["name", "string"],
        ["slug", "string"],
        ["timestamp", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_product_attributes(list):
    """
    This function builds and uploads a product_attributes dataframe
    """
    logger = logging.getLogger("primary_logger")
    table_name = "woo_api_product_attributes"
    data = list
    schema = [
        ["store_wid", "integer"],
        ["rls_value", "string"],
        ["sync_timestamp", "timestamp"],
        ["product_id", "integer"],
        ["id", "integer"],
        ["name", "string"],
        ["slug", "string"],
        ["option", "string"],
        ["timestamp", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_refunds(list):
    """
    This function builds and uploads a refund dataframe
    """
    logger = logging.getLogger("primary_logger")
    table_name = "woo_api_refunds"
    data = list
    schema = [
        ["store_wid", "integer"],
        ["rls_value", "string"],
        ["refund_number", "integer"],
        ["sync_timestamp", "timestamp"],
        ["agent_email", "string"],
        ["agent_name", "string"],
        ["date_created", "datetime"],
        ["date_modified", "datetime"],
        ["order_number", "integer"],
        ["parent_id", "integer"],
        ["shipping", "bignumeric"],
        ["shipping_tax", "bignumeric"],
        ["subtotal", "bignumeric"],
        ["subtotal_tax", "bignumeric"],
        ["timestamp", "integer"],
        ["total", "bignumeric"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df['date_created'] =  pd.DataFrame({'dates': pd.to_datetime(df['date_created'])})
    df['date_modified'] =  pd.DataFrame({'dates': pd.to_datetime(df['date_modified'])})

    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_refund_items(list):
    """
    This function builds and uploads a refund_items dataframe
    """
    logger = logging.getLogger("primary_logger")
    table_name = "woo_api_refund_items"
    data = list
    schema = [
        ["store_wid", "integer"],
        ["rls_value", "string"],
        ["refund_item_id", "integer"],
        ["sync_timestamp", "timestamp"],
        ["date_created", "datetime"],
        ["order_number", "integer"],
        ["order_item_id", "integer"],
        ["product_component_cost", "bignumeric"],
        ["product_cost", "bignumeric"],
        ["product_id", "integer"],
        ["product_name", "string"],
        ["product_price", "bignumeric"],
        ["product_quantity", "integer"],
        ["product_sku", "string"],
        ["product_tax", "bignumeric"],
        ["refund_number", "integer"],
        ["timestamp", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df['date_created'] =  pd.DataFrame({'dates': pd.to_datetime(df['date_created'])})
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

## BUILD LISTS -----------------------------------------------------------------------------------------------------------------------------------------
def orders(o, order_list, env_var_list):
    """
    This function builds a order record
    """
    list = []
    list.append(int(env_var_list["store_wid"]))
    list.append(env_var_list["rls_value"]) 
    list.append(o['id'])
    list.append(env_var_list["sync_timestamp"])
    list.append(o['cru_data']['agent']['email'])
    list.append(o['cru_data']['agent']['name'])
    list.append(o['billing']['address_1'])
    list.append(o['billing']['address_2'])
    list.append(o['billing']['city'])
    list.append(o['billing']['company'])
    list.append(o['billing']['country'])
    list.append(o['billing']['email'])
    list.append(o['billing']['first_name'])
    list.append(o['billing']['last_name'])
    list.append(o['billing']['phone'])
    list.append(o['billing']['postcode'])
    list.append(o['billing']['state'])
    list.append(o['cart_hash'])
    list.append(Decimal(str(o['cart_tax'])))
    list.append(o['created_via'])
    list.append(o['currency'])
    list.append(o['cru_data']['shipping']['custom_note'])
    list.append(o['customer_id'])
    list.append(o['customer_ip_address'])
    list.append(o['customer_note'])
    list.append(o['cru_data']['customer_role'])
    list.append(o['customer_user_agent'])
    list.append(o['date_completed'])
    list.append(o['date_created'])
    list.append(o['date_modified'])
    list.append(o['date_paid'])
    list.append(o['cru_data']['shipping']['date_shipped']) 

    discount_amount = 0
    discount_code = ''
    discount_type = ''
    discount_description = ''
    cd = o['cru_data']
    for x in reversed(cd['discounts']):
        discount_amount = x['amount']
        discount_code = x['code']
        discount_type = x['type']
        discount_description = x['description']

    list.append(Decimal(str(discount_amount)))
    list.append(discount_code)
    list.append(discount_type)  
    list.append(discount_description) 


    list.append(Decimal(str(o['discount_tax'])))
    list.append(Decimal(str(o['discount_total'])))


    event_code = ''
    for y in o['meta_data']:
        if y['key'] == "event_code":
            event_code = y['value']
    list.append(event_code)

    list.append(o['order_key'])

    order_origin = ''
    for y in o['meta_data']:
        if y['key'] == "cru_order_origin":
            order_origin = y['value']
    list.append(order_origin)

    list.append(o['order_type'])
    list.append(o['cru_data']['ordered_by']['email'])
    list.append(o['cru_data']['ordered_by']['name'])
    list.append(o['cru_data']['ordered_by']['phone'])
    list.append(o['parent_id'])
    list.append(o['payment_method'])
    list.append(o['payment_method_title'])
    list.append(o['cru_data']['po_number'])
    list.append(o['prices_include_tax'])
    list.append(o['cru_data']['radio_station']['id'])
    list.append(o['cru_data']['radio_station']['description'])
    list.append(o['cru_data']['salesforce_account'])
    list.append(o['salesforce_id'])
    list.append(o['cru_data']['shipping']['shipped_method'])
    list.append(o['shipping']['address_1'])
    list.append(o['shipping']['address_2'])
    list.append(o['shipping']['city'])
    list.append(o['shipping']['company'])
    list.append(o['shipping']['country'])
    list.append(o['shipping']['first_name'])
    list.append(o['shipping']['last_name'])
    list.append(o['cru_data']['shipping']['method_id'])
    list.append(o['cru_data']['shipping']['method_title'])
    list.append(o['shipping']['postcode'])
    list.append(o['shipping']['state'])
    list.append(Decimal(str(o['shipping_tax'])))
    list.append(Decimal(str(o['shipping_total'])))
    list.append(o['status'])
    list.append(int(time.time()))
    list.append(Decimal(str(o['total'])))
    list.append(Decimal(str(o['total_tax'])))
    list.append(o['transaction_id'])
    list.append(o['version'])
    
    order_list.append(list)

def order_items(o, order_item_list, env_var_list):
    """
    This function loops through an order's line_items and pulls out needed info
    """
    donor_premium = 'false'
    sku = ''
    for li in o['line_items']:

        if li['bundled_by'] == "":
            sku = li['sku']
        
        list = []
        list.append(int(env_var_list["store_wid"]))
        list.append(env_var_list["rls_value"]) 
        list.append(li['id'])
        list.append(env_var_list["sync_timestamp"]) 
        list.append(o['date_created'])
        list.append(o['order_key'])
        list.append(o['id'])
        
        brand = ''
        if 'brand' in li:
            brand = li['brand']
        list.append(brand)

        component_cost = ''
        component_id = 0
        component_msrp = ''
        component_regular_price = ''
        component_sku = ''
        if 'cru_data' in li:
            cdc = li['cru_data']['component']
            component_cost = cdc['cost']
            component_id = cdc['id']
            component_msrp = cdc['msrp']
            component_regular_price = cdc['regular_price']         
            component_sku = cdc['sku']            
        list.append(Decimal(str(component_cost)))
        list.append(component_id)
        list.append(Decimal(str(component_msrp)))
        list.append(Decimal(str(component_regular_price)))
        list.append(component_sku)
        
        cost = 0
        for y in li['meta_data']:
            if y['key'] == "_alg_wc_cog_item_cost":
                cost = y['value'] 
        if not isinstance(cost, (int, float)):
            cost = 0         
        list.append(Decimal(str(cost)))
       
        dept = ''
        if 'dept' in li:
            dept = li['dept']
        list.append(dept) 

        discount = '0.00'
        exclude_discounting = ''
        free_shipping = ''
        gift_card = ''
        msrp = ''
        next_receipt_date = ''
        regular_price = ''
        royalty = ''
        if 'cru_data' in li:
            cd = li['cru_data']
            discount = cd['discount']
            if donor_premium == "false":
                donor_premium = cd['donor_premium']
            exclude_discounting = cd['exclude_discounting']
            free_shipping = cd['free_shipping']
            gift_card = cd['gift_card']
            msrp = cd['msrp']
            next_receipt_date = cd['next_receipt_date']
            regular_price = cd['regular_price']
            royalty = cd['royalty']
        list.append(Decimal(str(discount)))
        list.append(donor_premium)  
        list.append(exclude_discounting) 
        list.append(free_shipping)  
        list.append(gift_card)
        list.append(li['product_id'])

        impact = ''
        if 'impact' in li:
            impact = li['impact']
        list.append(impact) 

        product_inactive = ''
        if 'product_inactive' in li:
            product_inactive = li['product_inactive']
        list.append(product_inactive) 

        list.append(Decimal(str(msrp)))
        list.append(li['name'])
        list.append(next_receipt_date)
        list.append(Decimal(str(li['price'])))

        project = ''
        if 'project' in li:
            project = li['project']
        list.append(project) 

        list.append(li['quantity'])
        list.append(Decimal(str(regular_price)))
        list.append(royalty)
        list.append(sku)

        sub_brand = ''
        if 'sub_brand' in li:
            sub_brand = li['sub_brand']
        list.append(sub_brand)         

        list.append(Decimal(str(li['total_tax'])))

        total_manuals = ''
        if 'total_manuals' in li:
            total_manuals = li['total_manuals']
        list.append(total_manuals) 

        weight = '0.00'
        if li['weight'] != "":
            weight = li['weight'] 
        list.append(Decimal(str(weight)))

        per_case = 0
        if 'per_case' in li:
            if li['per_case'] != "":
                per_case = li['per_case'] 
        list.append(int(per_case)) 

        bundled_by = 0
        if li['bundled_by'] != "":
            bundled_by = int(str(li['bundled_by']))
        list.append(bundled_by)

        list.append(int(time.time()))

        order_item_list.append(list)

def products(p, product_list, env_var_list):
    """
    This function builds a product record
    """
    list = []

    exclude_from_all_discounting = 0
    free_shipping = 0
    product_inactive = 0
    gift_card = 0
    donor_premium = 0
    next_receipt_date = ''
    alg_wc_cog_cost = '0.00'
    fl_staff_price_field = '0.00'
    case_qty = 0
    product_page_count = 0
    product_isbn = ''
    product_publisher = ''
    brand = ''
    royalty = 0
    impact = ''
    product_language = ''
    sub_brand = ''
    total_manuals = 0
    for y in p['meta_data']:
        if y['key'] == "exclude_from_all_discounting":
            if y['value'] == "1":
                exclude_from_all_discounting = 1
        elif y['key'] == "free_shipping":
            if y['value'] == "1":
                free_shipping = 1
        elif y['key'] == "product_inactive":
            if y['value'] == "1":
                product_inactive = 1
        elif y['key'] == "gift_card":
            if y['value'] == "1":
                gift_card = 1
        elif y['key'] == "donor_premium":
            if y['value'] == "1":
                donor_premium = 1
        elif y['key'] == "next_receipt_date":
            next_receipt_date = y['value'] 
        elif y['key'] == "_alg_wc_cog_cost":
            if y['value'] != "":
                alg_wc_cog_cost = y['value']
        elif y['key'] == "fl_staff_price_field":
            if y['value'] != "":
                fl_staff_price_field = y['value']
        elif y['key'] == "case_qty":
            if y['value'] != "":
                case_qty = int(y['value'])
        elif y['key'] == "product_page_count":
            if y['value'] != "":
                product_page_count = int(y['value'])
        elif y['key'] == "product_isbn":
            product_isbn = y['value'] 
        elif y['key'] == "product_publisher":
            product_publisher = y['value'] 
        elif y['key'] == "brand":
            brand = y['value'] 
        elif y['key'] == "royalty":
            if y['value'] == "1":
                royalty = 1
        elif y['key'] == "impact":
            impact = y['value']
        elif y['key'] == "product_language":
            product_language = y['value'] 
        elif y['key'] == "sub_brand":
            sub_brand = y['value']
        elif y['key'] == "total_manuals":
            if y['value'] != "":
                total_manuals = int(y['value'])
    
    list.append(int(env_var_list["store_wid"]))
    list.append(env_var_list["rls_value"])
    list.append(env_var_list["sync_timestamp"])
    if p['date_created'] is not None:
        list.append(p['date_created'])
    else:
        list.append(p['date_modified'])
    list.append(p['date_modified'])

    list.append(p['id'])
    list.append(p['name'])
    list.append(p['short_description'])

    backorders_allowed = '0'
    if 'backorders_allowed' in p:
        if p['backorders_allowed'] != "":
            backorders_allowed = p['backorders_allowed'] 
    list.append(backorders_allowed)

    downloadable = 0
    if 'downloadable' in p:
        if p['downloadable'] != "":
            downloadable = p['downloadable'] 
    list.append(downloadable)

    virtual = 0
    if 'virtual' in p:
        if p['virtual'] != "":
            virtual = p['virtual'] 
    list.append(virtual)

    list.append(exclude_from_all_discounting)
    list.append(free_shipping)
    list.append(product_inactive)
    list.append(gift_card)
    list.append(donor_premium)
    list.append(royalty)
    list.append(next_receipt_date)

    list.append(brand)
    list.append(product_isbn)
    list.append(product_publisher)
    list.append(impact)
    list.append(product_language)
    list.append(sub_brand)
    status = str(p['status'])
    list.append(status.title())
    list.append(Decimal(str(alg_wc_cog_cost))) 
    list.append(Decimal(str(fl_staff_price_field))) 

    msrp_price = '0.00'
    if 'msrp_price' in p:
        if p['msrp_price'] != "":
            msrp_price = p['msrp_price'] 
    list.append(Decimal(str(msrp_price)))

    price = '0.00'
    if p['price'] != "":
        price = p['price'] 
    list.append(Decimal(str(price)))

    regular_price = '0.00'
    if p['regular_price'] != "":
        regular_price = p['regular_price'] 
    list.append(Decimal(str(regular_price)))

    list.append(p['sku'])

    weight = 0
    if p['weight'] != "":
        weight = p['weight'] 
    list.append(Decimal(str(weight)))

    list.append(p['type'])
    list.append(p['stock_quantity'])
    list.append(case_qty)
    list.append(product_page_count)
    list.append(total_manuals)
    list.append(int(time.time()))

    product_list.append(list)

def product_bundles(p, bundle_list, env_var_list):
    """
    This function loops through a refund's bundled_items and pulls out needed info
    """
    if 'bundled_items' in p:
        for i in p['bundled_items']:
            
            list = []
            list.append(int(env_var_list["store_wid"]))
            list.append(env_var_list["rls_value"]) 
            list.append(env_var_list["sync_timestamp"]) 
            list.append(Decimal(str(p['id'])))
            list.append(Decimal(str(i['bundled_item_id'])))
            list.append(Decimal(str(i['product_id'])))
            list.append(int(i['quantity_default']))
            list.append(int(time.time()))

            bundle_list.append(list)

def product_categories(p, category_list, env_var_list):
    """
    This function loops through a refund's categories and pulls out needed info
    """
    if 'categories' in p:
        for i in p['categories']:
            
            list = []
            list.append(int(env_var_list["store_wid"]))
            list.append(env_var_list["rls_value"]) 
            list.append(env_var_list["sync_timestamp"]) 
            list.append(p['id'])
            list.append(i['id'])
            list.append(i['name'])
            list.append(i['slug'])
            list.append(int(time.time()))

            category_list.append(list)

def product_attributes(p, attribute_list, env_var_list):
    """
    This function loops through a refund's attributes and pulls out needed info
    """
    if 'attributes' in p:
        for i in p['attributes']:
            
            list = []
            list.append(int(env_var_list["store_wid"]))
            list.append(env_var_list["rls_value"]) 
            list.append(env_var_list["sync_timestamp"]) 
            list.append(p['id'])
            list.append(i['id'])
            list.append(i['name'])
            list.append(i['slug'])
            list.append(i['options'][0])
            list.append(int(time.time()))

            attribute_list.append(list)

def refunds(r, refund_list, env_var_list):
    """
    This function builds a refund record
    """
    list = []
    list.append(int(env_var_list["store_wid"]))
    list.append(env_var_list["rls_value"])
    list.append(r['id'])
    list.append(env_var_list["sync_timestamp"])
    
    agent_email = ''
    agent_name = ''
    if 'cru_data' in r:
        cd = r['cru_data']
        agent_email = cd['agent_email']        
        agent_name = cd['agent_name']            
    list.append(agent_email)
    list.append(agent_name)

    list.append(r['date_created'])       
    list.append(r['date_created']) #date_modified
    list.append(r['parent_id']) #order_number
    list.append(r['parent_id'])

    shipping = 0
    shipping_tax = 0
    if 'shipping_lines' in r:
        for y in r['shipping_lines']:
            shipping =  y['total']
            shipping_tax = y['total_tax']
    list.append(Decimal(str(shipping))) 
    list.append(Decimal(str(shipping_tax)))

    subtotal = 0
    subtotal_tax = 0
    if 'cru_data' in r:
        cd = r['cru_data']
        subtotal = cd['subtotal']
        subtotal_tax = cd['subtotal_tax']              
    list.append(Decimal(str(subtotal)))
    list.append(Decimal(str(subtotal_tax)))

    list.append(int(time.time()))

    list.append(Decimal(str(r['amount']))) 

    refund_list.append(list)

def refund_items(r, refund_item_list, env_var_list):
    """
    This function loops through a refund's line_items and pulls out needed info
    """
    for li in r['line_items']:

        list = []
        list.append(int(env_var_list["store_wid"]))
        list.append(env_var_list["rls_value"])
        list.append(li['id'])
        list.append(env_var_list["sync_timestamp"])
        list.append(r['date_created'])    
        list.append(r['parent_id']) #order_number

        order_item_id = 0
        for y in li['meta_data']:
            if y['key'] == "_refunded_item_id":
                order_item_id = y['value']
        list.append(int(order_item_id))  

        product_component_cost = 0
        product_cost = 0
        for y in li['meta_data']:
            if y['key'] == "_alg_wc_cog_item_cost":
                product_component_cost = y['value']
                product_cost = y['value']
        if not isinstance(product_component_cost, (int, float)):
            product_component_cost = 0  
        if not isinstance(product_cost, (int, float)):
            product_cost = 0             
        list.append(Decimal(str(product_component_cost)))               
        list.append(Decimal(str(product_cost)))

        list.append(li['product_id'])
        list.append(li['name'])
        list.append(Decimal(str(li['price'])))
        list.append(li['quantity'])
        list.append(li['sku'])
        list.append(Decimal(str(li['total_tax'])))
        list.append(r['id'])
        list.append(int(time.time()))
    
        refund_item_list.append(list)

## API CALLS -----------------------------------------------------------------------------------------------------------------------------------------
def get_orders_and_items(env_var_list):
    """
    This function is called from trigger_sync.  It queries the WooCommerce API for NEW orders and order_items
    """
    logger = logging.getLogger("primary_logger")
    last_update_date_time = env_var_list["order_last_update_date_time"]
    logger.info(last_update_date_time)
    url = env_var_list["orders_api_url"]
    
    headers = {
        "Accept": "application/json",
        "Authorization": "Basic "
        + base64.b64encode(f"{env_var_list["woo_api_client_id"]}:{env_var_list["woo_api_client_secret"]}".encode("ascii")).decode(
            "ascii"
        ),
    }

    order_list = []
    order_item_list = []
    current_page = 1
    total_pages = 1

    while current_page <= total_pages:
        logger.info(str(current_page) + ' of ' + str(total_pages))
        params = {"modified_after": last_update_date_time, "per_page": 100, "page": current_page}
        response = requests.get(url, headers=headers, params=params, timeout=60)
        if response.status_code != 200:
            logger.error(f"API Error: {str(response.status_code)}")
        else:
            total_pages = int(response.headers.get('X-WP-TotalPages', '1'))
            order_resp = response.json()
            for o in order_resp:
                orders(o, order_list, env_var_list)
                order_items(o, order_item_list, env_var_list)
        current_page += 1

    process_orders(order_list)
    logger.info(f"order_list count: {str(len(order_list))}")
    process_order_items(order_item_list)  
    logger.info(f"order_item_list count: {str(len(order_item_list))}")

def get_products_and_bundles(env_var_list):
    """
    This function is called from trigger_sync.  It queries the WooCommerce API for ALL prducts, bundles, categories, and attributes
    """
    logger = logging.getLogger("primary_logger")
    url = env_var_list["products_api_url"]

    headers = {
        "Accept": "application/json",
        "Authorization": "Basic "
        + base64.b64encode(f"{env_var_list["woo_api_client_id"]}:{env_var_list["woo_api_client_secret"]}".encode("ascii")).decode(
            "ascii"
        ),
    }

    product_list = []
    product_bundle_list = []
    product_category_list = []
    product_attribute_list = []
    current_page = 1
    total_pages = 1

    while current_page <= total_pages:
        logger.info(str(current_page) + ' of ' + str(total_pages))
        params = {"per_page": 100, "page": current_page}
        response = requests.get(url, headers=headers, params=params, timeout=60)
        if response.status_code != 200:
            logger.error(f"API Error: {str(response.status_code)}")
        else:
            total_pages = int(response.headers.get('X-WP-TotalPages', '1'))
            product_resp = response.json()
            for p in product_resp:
                products(p, product_list, env_var_list)
                product_bundles(p, product_bundle_list, env_var_list)
                product_categories(p, product_category_list, env_var_list)
                product_attributes(p, product_attribute_list, env_var_list)
        current_page += 1
        
    process_products(product_list)
    logger.info(f"product_list count: {str(len(product_list))}")

    process_product_bundles(product_bundle_list)
    logger.info(f"product_bundle_list count: {str(len(product_bundle_list))}")

    process_product_categories(product_category_list)
    logger.info(f"product_category_list count: {str(len(product_category_list))}")

    process_product_attributes(product_attribute_list)
    logger.info(f"product_attribute_list count: {str(len(product_attribute_list))}")

def get_refunds_and_items(env_var_list):
    """
    This function is called from trigger_sync.  It queries the WooCommerce API for ALL refunds and refund_items
    """
    logger = logging.getLogger("primary_logger")
    url = env_var_list["refunds_api_url"]

    headers = {
        "Accept": "application/json",
        "Authorization": "Basic "
        + base64.b64encode(f"{env_var_list["woo_api_client_id"]}:{env_var_list["woo_api_client_secret"]}".encode("ascii")).decode(
            "ascii"
        ),
    }

    refund_list = []
    refund_item_list = []
    current_page = 1
    total_pages = 1

    while current_page <= total_pages:
        logger.info(str(current_page) + ' of ' + str(total_pages))
        params = {"per_page": 100, "page": current_page}
        response = requests.get(url, headers=headers, params=params, timeout=60)
        if response.status_code != 200:
            logger.error(f"API Error: {str(response.status_code)}")
        else:
            total_pages = int(response.headers.get('X-WP-TotalPages', '1'))
            refund_resp = response.json()
            for r in refund_resp:
                refunds(r, refund_list, env_var_list)
                refund_items(r, refund_item_list, env_var_list)
        current_page += 1
    
    process_refunds(refund_list)
    logger.info(f"refund_list count: {str(len(refund_list))}")
    process_refund_items(refund_item_list)
    logger.info(f"refund_item_list count: {str(len(refund_item_list))}")

## MAIN -----------------------------------------------------------------------------------------------------------------------------------------
def trigger_sync():
    """
    Main entry point for Cloud Run Jobs.
    This function will be called when the job is triggered.
    """
    logger = logging.getLogger("primary_logger")
    logger.info("Starting Woo API data synchronization job")

    log_memory_usage("- Job Start")

    try:
        sync_timestamp = str(datetime.now(timezone.utc))
        
        # CRU Store --------------------------------
        cru_order_last_update_date_time = get_last_load_date_time(GET_CRU_LAST_LOAD_ORDERS)
        env_var_dict_cru = {
            "sync_timestamp": sync_timestamp,
            "order_last_update_date_time": cru_order_last_update_date_time,
            "store_wid": os.getenv("CRU_STORE_WID", None),
            "rls_value": os.environ.get("CRU_RLS_VALUE", None),
            "woo_api_client_id": os.environ.get("API_CLIENT_ID", None),
            "woo_api_client_secret": os.environ.get("API_CLIENT_SECRET", None),
            "orders_api_url": os.environ.get("CRU_API_ORDERS", None),
            "products_api_url": os.environ.get("CRU_API_PRODUCTS", None),
            "refunds_api_url": os.environ.get("CRU_API_REFUNDS", None),
        }
        env_var_string = json.dumps(env_var_dict_cru)
        env_var_list = json.loads(env_var_string)

        logger.info("BEGIN - CRU order sync")    
        get_orders_and_items(env_var_list)

        logger.info("BEGIN - CRU refund sync")    
        get_refunds_and_items(env_var_list)

        logger.info("BEGIN - CRU product sync")    
        get_products_and_bundles(env_var_list)
        
        # FamilyLife Store --------------------------------
        fl_order_last_update_date_time = get_last_load_date_time(GET_FL_LAST_LOAD_ORDERS)
        env_var_dict_fl = {
            "sync_timestamp": sync_timestamp,
            "order_last_update_date_time": fl_order_last_update_date_time,
            "store_wid": os.getenv("FL_STORE_WID", None),
            "rls_value": os.environ.get("FL_RLS_VALUE", None),
            "woo_api_client_id": os.environ.get("API_CLIENT_ID", None),
            "woo_api_client_secret": os.environ.get("API_CLIENT_SECRET", None),
            "orders_api_url": os.environ.get("FL_API_ORDERS", None),
            "products_api_url": os.environ.get("FL_API_PRODUCTS", None),
            "refunds_api_url": os.environ.get("FL_API_REFUNDS", None),
        }
        env_var_string = json.dumps(env_var_dict_fl)
        env_var_list = json.loads(env_var_string)
    
        logger.info("BEGIN - FamilyLife order sync")  
        get_orders_and_items(env_var_list)

        logger.info("BEGIN - FamilyLife refund sync")  
        get_refunds_and_items(env_var_list)
        
        logger.info("BEGIN - FamilyLife product sync")  
        get_products_and_bundles(env_var_list)

    except Exception as e:
        logger.exception(f"Error processing WooCommerce Api: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    setup_logging()
    trigger_sync()


