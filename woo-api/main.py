import os
import requests
import json
import logging
import sys
import pandas as pd
import base64
import time
from datetime import datetime
from bigquery_client import BigQueryClient
from typing import Tuple, List, Dict, Any
from urllib.parse import urlparse
from google.cloud import bigquery

logger = logging.getLogger("primary_logger")
logger.propagate = False
project_name = os.environ.get("BIGQUERY_PROJECT_NAME", None)
dataset_name = os.environ.get("BIGQUERY_DATASET_NAME", None)
timestamp_client = bigquery.Client()
client = BigQueryClient(project=project_name)

GET_LAST_LOAD_ORDERS= """
    select sync_timestamp 
    from `cru-dw-devs-chad-kline.el_woocommerce_api.woo_api_orders`
    order by 1 desc 
    limit 1
"""

GET_LAST_LOAD_PRODUCTS = """
    select sync_timestamp 
    from `cru-dw-devs-chad-kline.el_woocommerce_api.woo_api_products`
    order by 1 desc 
    limit 1
"""

GET_LAST_LOAD_REFUNDS = """
    select sync_timestamp 
    from `cru-dw-devs-chad-kline.el_woocommerce_api.woo_api_refunds`
    order by 1 desc 
    limit 1
"""

GET_LAST_LOAD_POSTS = """
    select sync_timestamp 
    from `cru-dw-devs-chad-kline.el_woocommerce_api.woo_api_posts`
    order by 1 desc 
    limit 1
"""

## SCRIPT UTILITIES -------------------------------------------------------------------------------------------------------------------------------------------
class CloudLoggingFormatter(logging.Formatter):
    """
    Produces messages compatible with google cloud logging
    """

    def format(self, record: logging.LogRecord) -> str:
        s = super().format(record)
        return json.dumps(
            {
                "message": s,
                "severity": record.levelname,
                "timestamp": {"seconds": int(record.created), "nanos": 0},
            }
        )

def setup_logging():
    """
    Sets up logging for the application.
    """
    global logger

    # Remove any existing handlers
    if logger.handlers:
        for handler in logger.handlers:
            logger.removeHandler(handler)

    handler = logging.StreamHandler(stream=sys.stdout)
    formatter = CloudLoggingFormatter(fmt="%(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    sys.excepthook = handle_unhandled_exception

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
        "bool": "boolean"
    }

def create_dtype_dict(schema: list, dtype_mapping: Dict[str, str]) -> Dict[int, str]:
    """Create a dictionary mapping column indices to their data types."""
    return {i: dtype_mapping[col_type] for i, (_, col_type) in enumerate(schema)}

def load_to_dataframe(
    data: list,
    schema: list,
    skip_header_rows: int = 1,
) -> pd.DataFrame:
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

    query = obj
    job_config = bigquery.QueryJobConfig()

    try:
        query_job = timestamp_client.query(query, job_config=job_config)
        result = query_job.result()
        row = list(result)[0]
        last_update = row.sync_timestamp
        last_update = datetime.fromisoformat(str(last_update)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        return last_update

    except Exception as e:
        print(f"Get last load date error: {str(e)}")

## BQ DATAFRAMES -------------------------------------------------------------------------------------------------------------------------------------------
def process_orders(list):
    table_name = "woo_api_orders"
    data = list
    schema = [
        ["store_wid", "string"],
        ["rls_value", "string"],
        ["order_item_id", "integer"],
        ["_fivetran_deleted", "string"],
        ["sync_timestamp", "datetime"],
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
        ["cart_tax", "string"],
        ["created_via", "string"],
        ["currency", "string"],
        ["custom_shipping_note", "string"],
        ["customer_id", "integer"],
        ["customer_ip_address", "string"],
        ["customer_note", "string"],
        ["customer_role", "string"],
        ["customer_user_agent", "string"],
        ["date_completed", "date"],
        ["date_created", "date"],
        ["date_modified", "date"],
        ["date_paid", "date"],
        ["date_shipped", "string"],
        ["discount_amount", "string"],
        ["discount_codes", "string"],
        ["discount_description", "string"],
        ["discount_tax", "string"],
        ["discount_total", "string"],
        ["discount_type", "string"],
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
        ["shipping_tax", "string"],
        ["shipping_total", "string"],
        ["status", "string"],
        ["timestamp", "integer"],
        ["total", "string"],
        ["total_tax", "string"],
        ["transaction_id", "string"],
        ["version", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df["date_completed"] = pd.to_datetime(df["date_completed"]).dt.date
    df["date_created"] = pd.to_datetime(df["date_created"]).dt.date
    df["date_modified"] = pd.to_datetime(df["date_modified"]).dt.date
    df["date_paid"] = pd.to_datetime(df["date_paid"]).dt.date
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_order_items(list):
    table_name = "woo_api_order_items"
    data = list
    schema = [
        ["store_wid", "string"],
        ["rls_value", "string"],
        ["order_item_id", "integer"],
        ["_fivetran_deleted", "string"],
        ["sync_timestamp", "datetime"],
        ["date_created", "date"],
        ["order_key", "string"],
        ["order_number", "integer"],
        ["product_brand", "string"],
        ["product_component_cost", "float"],
        ["product_component_id", "string"],
        ["product_component_msrp", "float"],
        ["product_component_regular_price", "float"],
        ["product_component_sku", "string"],
        ["product_cost", "string"],
        ["product_dept", "string"],
        ["product_discount", "float"],
        ["product_donor_premium", "bool"],
        ["product_exclude_discounting", "string"],
        ["product_free_shipping", "string"],
        ["product_gift_card", "string"],
        ["product_id", "integer"],
        ["product_impact", "string"],
        ["product_inactive", "string"],
        ["product_msrp", "float"],
        ["product_name", "string"],
        ["product_next_receipt_date", "string"],
        ["product_price", "float"],
        ["product_project", "string"],
        ["product_quantity", "integer"],
        ["product_regular_price", "float"],
        ["product_royalty", "string"],
        ["product_sku", "string"],
        ["product_subbrand", "string"],
        ["product_tax", "string"],
        ["product_total_manuals", "string"],
        ["product_weight", "string"],
        ["products_per_case", "string"],
        ["timestamp", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df["date_created"] = pd.to_datetime(df["date_created"]).dt.date
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_products(list):
    table_name = "woo_api_products"
    data = list
    schema = [
        ["store_wid", "string"],
        ["rls_value", "string"],
        ["sync_timestamp", "datetime"],
        ["id", "integer"],
        ["brand", "string"],
        ["status", "string"],
        ["msrp_price", "string"],
        ["price", "string"],
        ["regular_price", "string"],
        ["sku", "string"],
        ["weight", "string"],
        ["type", "string"],
        ["stock_quantity", "integer"],
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
    table_name = "woo_api_refunds"
    data = list
    schema = [
        ["store_wid", "string"],
        ["rls_value", "string"],
        ["refund_number", "integer"],
        ["_fivetran_deleted", "string"],
        ["sync_timestamp", "datetime"],
        ["agent_email", "string"],
        ["agent_name", "string"],
        ["currency", "string"],
        ["date_created", "date"],
        ["date_modified", "date"],
        ["order_key", "string"],
        ["order_number", "integer"],
        ["parent_id", "integer"],
        ["shipping", "string"],
        ["shipping_tax", "string"],
        ["status", "string"],
        ["subtotal", "float"],
        ["subtotal_tax", "float"],
        ["timestamp", "integer"],
        ["total", "string"],
        ["version", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df["date_created"] = pd.to_datetime(df["date_created"]).dt.date
    df["date_modified"] = pd.to_datetime(df["date_modified"]).dt.date
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_refund_items(list):
    table_name = "woo_api_refund_items"
    data = list
    schema = [
        ["store_wid", "string"],
        ["rls_value", "string"],
        ["refund_item_id", "integer"],
        ["_fivetran_deleted", "string"],
        ["sync_timestamp", "datetime"],
        ["date_created", "date"],
        ["order_key", "string"],
        ["order_number", "integer"],
        ["product_component_cost", "string"],
        ["product_component_id", "string"],
        ["product_component_msrp", "string"],
        ["product_component_regular_price", "string"],
        ["product_component_sku", "string"],
        ["product_cost", "string"],
        ["product_dept", "string"],
        ["product_discount", "string"],
        ["product_donor_premium", "string"],
        ["product_exclude_discounting", "string"],
        ["product_free_shipping", "string"],
        ["product_gift_card", "string"],
        ["product_id", "integer"],
        ["product_impact", "string"],
        ["product_inactive", "string"],
        ["product_name", "string"],
        ["product_next_receipt_date", "string"],
        ["product_project", "string"],
        ["product_quantity", "integer"],
        ["product_royalty", "string"],
        ["product_subbrand", "string"],
        ["product_tax", "string"],
        ["product_total_manuals", "string"],
        ["products_per_case", "string"],
        ["refund_number", "integer"],
        ["timestamp", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df["date_created"] = pd.to_datetime(df["date_created"]).dt.date
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

def process_posts(list):
    table_name = "woo_api_posts"
    data = list
    schema = [
        ["store_wid", "string"],
        ["rls_value", "string"],
        ["sync_timestamp", "datetime"],
        ["id", "integer"],
        ["post_content", "string"],
        ["post_date", "date"],
        ["post_modified", "date"],
        ["post_name", "string"],
        ["post_status", "string"],
        ["post_title", "string"],
        ["post_type", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(data, schema, skip_header_rows=0)
    df['sync_timestamp'] =  pd.DataFrame({'dates': pd.to_datetime(df['sync_timestamp'])})
    df["post_date"] = pd.to_datetime(df["post_date"]).dt.date
    df["post_modified"] = pd.to_datetime(df["post_modified"]).dt.date
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )

## BUILD LISTS -----------------------------------------------------------------------------------------------------------------------------------------
def orders(o, order_list, env_var_list):
    list = []
    list.append(env_var_list["store_wid"])
    list.append(env_var_list["rls_value"]) 
    list.append(o['id'])
    list.append("false") #_fivetran_deleted
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
    list.append(o['cart_tax'])
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
    list.append("discount_amount") #discount_amount
    list.append("discount_codes") #discount_codes
    list.append("discount_description") #discount_description
    list.append(o['discount_tax'])
    list.append(o['discount_total'])
    list.append("discount_type") #discount_type

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
    list.append(o['shipping_tax'])
    list.append(o['shipping_total'])
    list.append(o['status'])
    list.append(int(time.time()))
    list.append(o['total'])
    list.append(o['total_tax'])
    list.append(o['transaction_id'])
    list.append(o['version'])
    
    order_list.append(list)

def order_items(o, order_item_list, env_var_list):
    for li in o['line_items']:
        
        list = []
        list.append(env_var_list["store_wid"]) 
        list.append(env_var_list["rls_value"]) 
        list.append(li['id'])
        list.append("false") #_fivetran_deleted
        list.append(env_var_list["sync_timestamp"]) 
        list.append(o['date_created'])
        list.append(o['order_key'])
        list.append(o['id'])
        
        brand = ''
        if 'brand' in li:
            brand = li['brand']
        list.append(brand)

        component_cost = ''
        component_id = ''
        component_msrp = ''
        component_regular_price = ''
        component_sku = ''
        if 'cru_data' in li:
            component_cost = li['cru_data']['component']['cost']
            component_id = li['cru_data']['component']['id']
            component_msrp = li['cru_data']['component']['msrp']
            component_regular_price = li['cru_data']['component']['regular_price']         
            component_sku = li['cru_data']['component']['sku']            
        list.append(component_cost)
        list.append(component_id)
        list.append(component_msrp)
        list.append(component_regular_price)
        list.append(component_sku)
        
        cost = ''
        if 'cost' in li:
            cost = li['cost']
        list.append(cost) 
       
        dept = ''
        if 'dept' in li:
            dept = li['dept']
        list.append(dept) 

        discount = ''
        donor_premium = ''
        exclude_discounting = ''
        free_shipping = ''
        gift_card = ''
        msrp = ''
        next_receipt_date = ''
        regular_price = ''
        royalty = ''
        if 'cru_data' in li:
            discount = li['cru_data']['discount']
            donor_premium = li['cru_data']['donor_premium']
            exclude_discounting = li['cru_data']['exclude_discounting']
            free_shipping = li['cru_data']['free_shipping']
            gift_card = li['cru_data']['gift_card']
            msrp = li['cru_data']['msrp']
            next_receipt_date = li['cru_data']['next_receipt_date']
            regular_price = li['cru_data']['regular_price']
            royalty = li['cru_data']['royalty']
        list.append(discount)  
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

        list.append(msrp)
        list.append(li['name'])
        list.append(next_receipt_date)
        list.append(li['price'])

        project = ''
        if 'project' in li:
            project = li['project']
        list.append(project) 

        list.append(li['quantity'])
        list.append(regular_price)
        list.append(royalty)
        list.append(li['sku'])

        sub_brand = ''
        if 'sub_brand' in li:
            sub_brand = li['sub_brand']
        list.append(sub_brand)         

        list.append(li['total_tax'])

        total_manuals = ''
        if 'total_manuals' in li:
            total_manuals = li['total_manuals']
        list.append(total_manuals) 

        weight = ''
        if 'weight' in li:
            weight = li['weight']
        list.append(weight) 

        per_case = ''
        if 'per_case' in li:
            per_case = li['per_case']
        list.append(per_case) 

        list.append(int(time.time()))

        order_item_list.append(list)

def products(p, product_list, env_var_list):
    list = []

    list.append(env_var_list["store_wid"])
    list.append(env_var_list["rls_value"])
    list.append(env_var_list["sync_timestamp"])


    list.append(p['id'])

    brand = ''
    if 'gpf_data' in p:
        brand = p['gpf_data']['brand']
    list.append(brand)

    list.append(p['status'])

    msrp_price = ''
    if 'msrp_price' in p:
        msrp = p['msrp_price']
    list.append(msrp_price)

    list.append(p['price'])
    list.append(p['regular_price'])
    list.append(p['sku'])

    weight = ''
    if 'weight' in p:
        weight = p['weight']
    list.append(weight)

    list.append(p['type'])
    list.append(p['stock_quantity'])

    product_list.append(list)

def refunds(r, refund_list, env_var_list):
    list = []
    list.append(env_var_list["store_wid"])
    list.append(env_var_list["rls_value"])
    list.append(r['id'])
    list.append("false") #_fivetran_deleted
    list.append(env_var_list["sync_timestamp"])
    agent_email = 'agent_email'
    agent_name = 'agent_name'
    if 'agent' in r['cru_data']:
        agent_email = r['cru_data']['agent']['email']        
        agent_name = r['cru_data']['agent']['name']            
    list.append(agent_email)
    list.append(agent_name)   
    list.append("3.99") #currency
    list.append(r['date_created'])       
    list.append(r['date_created']) #date_modified
    list.append("order_key") #order_key
    list.append(r['parent_id']) #order_number
    list.append(r['parent_id'])

    shipping = 'shipping'
    shipping_tax = 'shipping_tax'
    if 'shipping_lines' in r:
        for y in r['shipping_lines']:
            shipping = y['total']
            shipping_tax = y['total_tax']  
    list.append(shipping)
    list.append(shipping_tax)

    list.append("status") #status

    subtotal = 'subtotal'
    subtotal_tax = 'subtotal_tax'
    if 'cru_data' in r:
        subtotal = r['cru_data']['subtotal']
        subtotal_tax = r['cru_data']['subtotal_tax']              
    list.append(subtotal)
    list.append(subtotal_tax)

    list.append(int(time.time()))
    list.append(r['amount'])
    list.append("version") #version

    refund_list.append(list)

def refund_items(r, refund_item_list, env_var_list):
    for li in r['line_items']:

        list = []
        list.append(env_var_list["store_wid"])
        list.append(env_var_list["rls_value"])
        list.append(li['id'])
        list.append("false") #_fivetran_deleted
        list.append(env_var_list["sync_timestamp"])
        list.append(r['date_created'])    
        list.append("order_key") #order_key
        list.append(r['parent_id']) #order_number
        #PRODUCT-API.gpf_data.brand #product_brand

        product_component_cost = ''
        for y in li['meta_data']:
            if y['key'] == "_alg_wc_cog_item_cost":
                product_component_cost = y['value']
        list.append(product_component_cost)
        
        list.append("product_component_id") #product_component_id
        list.append("product_component_msrp") #product_component_msrp
        list.append("product_component_regular_price") #product_component_regular_price
        list.append("product_component_sku") #product_component_sku

        product_cost = ''
        for y in li['meta_data']:
            if y['key'] == "_alg_wc_cog_item_cost":
                product_cost = y['value']
        list.append(product_cost)

        list.append("product_dept") #product_dept
        list.append("product_discount") #product_discount
        list.append("product_donor_premium") #product_donor_premium
        list.append("product_exclude_discounting") #product_exclude_discounting
        list.append("product_free_shipping") #product_free_shipping
        list.append("product_gift_card") #product_gift_card

        list.append(li['product_id'])
        list.append("product_impact") #product_impact
        list.append("product_inactive") #product_inactive
        #PRODUCT-API.msrp_price #product_msrp
        list.append(li['name'])
        list.append("product_next_receipt_date") #product_next_receipt_date
        #PRODUCT-API.price #product_price
        list.append("product_project") #product_project
        list.append(li['quantity'])
        #PRODUCT-API.regular_price #product_regular_price
        list.append("product_royalty") #product_royalty
        #PRODUCT-API.sku #product_sku
        list.append("product_subbrand") #product_subbrand
        list.append(li['total_tax'])
        list.append("product_total_manuals") #product_total_manuals
        #PRODUCT-API.weight #product_weight
        list.append("products_per_case") #products_per_case
        list.append(r['id'])
        list.append(int(time.time()))
    
        refund_item_list.append(list)

def posts(p, product_list, env_var_list):
    list = []

    list.append(env_var_list["store_wid"])
    list.append(env_var_list["rls_value"])
    list.append(env_var_list["sync_timestamp"])
    list.append(p['id'])
    list.append(p['content']['rendered'])
    list.append(p['date'])
    list.append(p['modified'])
    list.append(p['slug'])
    list.append(p['status'])
    list.append(p['title']['rendered'])
    list.append(p['type'])

    product_list.append(list)

## API CALLS -----------------------------------------------------------------------------------------------------------------------------------------
def get_orders_and_items(env_var_list):

    last_update_date_time = get_last_load_date_time(GET_LAST_LOAD_ORDERS)
    print(last_update_date_time)
    
    url = env_var_list["orders_api_url"]

    headers = {
        "Accept": "application/json",
        "Authorization": "Basic "
        + base64.b64encode(f"{env_var_list["woo_api_client_id"]}:{env_var_list["woo_api_client_secret"]}".encode("ascii")).decode(
            "ascii"
        ),
    }
    params = {"modified_after": last_update_date_time}

    response = requests.get(url, headers=headers, params=params, timeout=60)

    order_list = []
    order_item_list = []
    if response.status_code != 200:
        logger.error(f"API Error: {str(response.status_code)}")
    else:
        order_resp = response.json()
        for o in order_resp:
            orders(o, order_list, env_var_list)
            order_items(o, order_item_list, env_var_list)

    process_orders(order_list)
    logger.info(f"order_list count: {str(len(order_list))}")
    process_order_items(order_item_list)  
    logger.info(f"order_item_list count: {str(len(order_item_list))}")

def get_products(env_var_list):

    last_update_date_time = get_last_load_date_time(GET_LAST_LOAD_PRODUCTS)
    print(last_update_date_time)

    url = env_var_list["products_api_url"]
    headers = {
        "Accept": "application/json",
        "Authorization": "Basic "
        + base64.b64encode(f"{env_var_list["woo_api_client_id"]}:{env_var_list["woo_api_client_secret"]}".encode("ascii")).decode(
            "ascii"
        ),
    }
    params = {"modified_after": last_update_date_time}

    response = requests.get(url, headers=headers, params=params, timeout=60)

    product_list = []
    if response.status_code != 200:
        logger.error(f"API Error: {str(response.status_code)}")
    else:
        product_resp = response.json()
        for p in product_resp:
            products(p, product_list, env_var_list)
    
    process_products(product_list)
    logger.info(f"product_list count: {str(len(product_list))}")

def get_refunds_and_items(env_var_list):

    last_update_date_time = get_last_load_date_time(GET_LAST_LOAD_REFUNDS)
    print(last_update_date_time)

    url = env_var_list["refunds_api_url"]
    headers = {
        "Accept": "application/json",
        "Authorization": "Basic "
        + base64.b64encode(f"{env_var_list["woo_api_client_id"]}:{env_var_list["woo_api_client_secret"]}".encode("ascii")).decode(
            "ascii"
        ),
    }
    params = {"modified_after": last_update_date_time}

    response = requests.get(url, headers=headers, params=params, timeout=60)

    refund_list = []
    refund_item_list = []
    if response.status_code != 200:
        logger.error(f"API Error: {str(response.status_code)}")
    else:
        refund_resp = response.json()
        for r in refund_resp:
            refunds(r, refund_list, env_var_list)
            refund_items(r, refund_item_list, env_var_list)
    
    process_refunds(refund_list)
    logger.info(f"refund_list count: {str(len(refund_list))}")
    process_refund_items(refund_item_list)
    logger.info(f"refund_item_list count: {str(len(refund_item_list))}")

def get_posts(env_var_list):

    last_update_date_time = get_last_load_date_time(GET_LAST_LOAD_POSTS)
    print(last_update_date_time)

    url = env_var_list["posts_api_url"]
    headers = {
        "Accept": "application/json",
        "Authorization": "Basic "
        + base64.b64encode(f"{env_var_list["woo_api_client_id"]}:{env_var_list["woo_api_client_secret"]}".encode("ascii")).decode(
            "ascii"
        ),
    }
    params = {"modified_after": last_update_date_time}

    response = requests.get(url, headers=headers, params=params, timeout=60)
    
    post_list = []
    if response.status_code != 200:
        logger.error(f"API Error: {str(response.status_code)}")
    else:
        post_resp = response.json()
        for p in post_resp:
            posts(p, post_list, env_var_list)

    process_posts(post_list)
    logger.info(f"post_list count: {str(len(post_list))}")

## MAIN -----------------------------------------------------------------------------------------------------------------------------------------
def main():
    sync_timestamp = str(datetime.now())
    setup_logging()
    try:
        
        # FamilyLife Store --------------------------------
        env_var_dict = {
            "sync_timestamp": sync_timestamp,
            "store_wid": os.environ.get("FL_STORE_WID", None),
            "rls_value": os.environ.get("FL_RLS_VALUE", None),
            "woo_api_client_id": os.environ.get("WOO_API_CLIENT_ID", None),
            "woo_api_client_secret": os.environ.get("WOO_API_CLIENT_SECRET", None),
            "orders_api_url": os.environ.get("FL_API_ORDERS", None),
            "products_api_url": os.environ.get("FL_API_PRODUCTS", None),
            "refunds_api_url": os.environ.get("FL_API_REFUNDS", None),
            "posts_api_url": os.environ.get("FL_API_POSTS", None)
        }
        env_var_string = json.dumps(env_var_dict)
        env_var_list = json.loads(env_var_string)
    
        get_orders_and_items(env_var_list)
        logger.info("FamilyLife order data sync completed")

        get_products(env_var_list)
        logger.info("FamilyLife product data sync completed")

        get_refunds_and_items(env_var_list)
        logger.info("FamilyLife Store refund sync completed")

        get_posts(env_var_list)
        logger.info("FamilyLife Store posts sync completed")

        # TODO: CRU Store --------------------------------


    except Exception as e:
        logger.exception(f"Error processing WooCommerce Api: {str(e)}")
        sys.exit(1)

main()

