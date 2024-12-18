import os
import requests
import re
import logging
from datetime import datetime
import sys
import functions_framework
import json
from urllib.parse import urlparse
from bigquery_client import BigQueryClient
import pandas as pd
import zipfile
from datetime import date
import io
from typing import Tuple, List, Dict, Any

logger = logging.getLogger("primary_logger")
logger.propagate = False

service_account_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
service_account = open(service_account_path, "r").read()
client = BigQueryClient(service_account)
dataset_name = "el_geography"


def env_var(name):
    return os.environ[name]


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


def get_authentication(url: str) -> Tuple[Dict[str, str], Any]:
    """Handle authentication for different domains."""
    url_domain = urlparse(url).netloc
    auth = None

    if "geonames" in url_domain:
        geonames_username = env_var("GEONAMES_USERNAME")
        geonames_password = env_var("GEONAMES_PASSWORD")
        auth = (geonames_username, geonames_password)
        return url, auth
    elif "maxmind" in url_domain:
        maxmind_license_key = env_var("MAXMIND_LICENSE_KEY")
        url = url + f"&license_key={maxmind_license_key}"
        return url, None

    return url, auth


def get_dtype_mapping() -> Dict[str, str]:
    """Return the mapping of schema types to pandas dtypes."""
    return {
        "string": "string",
        "integer": "Int64",
        "float": "float64",
        "object": "string",
        "date": "string",
    }


def get_na_values() -> List[str]:
    """Return the list of values to be treated as NA. This is to mainly exclude country code 'NA'."""
    return [
        "",
        " ",
        "#N/A",
        "#N/A N/A",
        "#NA",
        "-1.#IND",
        "-1.#QNAN",
        "-NaN",
        "-nan",
        "1.#IND",
        "1.#QNAN",
        "<NA>",
        "N/A",
        "NULL",
        "NaN",
        "None",
        "n/a",
        "nan",
        "null ",
    ]


def create_dtype_dict(schema: list, dtype_mapping: Dict[str, str]) -> Dict[int, str]:
    """Create a dictionary mapping column indices to their data types."""
    return {i: dtype_mapping[col_type] for i, (_, col_type) in enumerate(schema)}


def read_csv_from_bytes(
    file_bytes: io.BytesIO,
    sep: str,
    skip_header_rows: int,
    header: int,
    dtypes: Dict[int, str],
    num_columns: int,
    na_values: List[str],
) -> pd.DataFrame:
    """Read a CSV file from bytes into a pandas DataFrame."""
    return pd.read_csv(
        file_bytes,
        sep=sep,
        skiprows=skip_header_rows,
        header=header,
        on_bad_lines="skip",
        dtype=dtypes,
        usecols=range(num_columns),
        keep_default_na=False,
        na_values=na_values,
    )


def process_zip_file(
    zip_ref: zipfile.ZipFile,
    file_name_regex: str,
    sep: str,
    skip_header_rows: int,
    header: int,
    dtypes: Dict[int, str],
    num_columns: int,
    na_values: List[str],
) -> pd.DataFrame:
    """Process a ZIP file and return a DataFrame from the contained CSV."""
    if len(zip_ref.namelist()) == 1:
        file_name = zip_ref.namelist()[0]
    else:
        matched_files = [
            file_name
            for file_name in zip_ref.namelist()
            if re.match(file_name_regex, file_name)
        ]
        if not matched_files:
            raise ValueError("No regex matching file found in the ZIP archive.")
        file_name = matched_files[0]

    with zip_ref.open(file_name) as extracted_file:
        return read_csv_from_bytes(
            io.BytesIO(extracted_file.read()),
            sep,
            skip_header_rows,
            header,
            dtypes,
            num_columns,
            na_values,
        )


def load_to_dataframe(
    url: str,
    schema: list,
    sep: str = "\t",
    skip_header_rows: int = 1,
    header: int = None,
    file_name_regex: str = None,
) -> pd.DataFrame:
    """Main function to load data from URL into a DataFrame."""
    try:
        url, auth = get_authentication(url)

        dtype_mapping = get_dtype_mapping()
        na_values = get_na_values()
        dtypes = create_dtype_dict(schema, dtype_mapping)
        num_columns = len(schema)

        with requests.get(url, auth=auth, stream=True) as r:
            r.raise_for_status()
            file_bytes = io.BytesIO(r.content)

            # Process the file based on type
            if url.endswith(".zip") or "suffix=zip" in url:
                with zipfile.ZipFile(file_bytes, "r") as zip_ref:
                    df_original = process_zip_file(
                        zip_ref,
                        file_name_regex,
                        sep,
                        skip_header_rows,
                        header,
                        dtypes,
                        num_columns,
                        na_values,
                    )
            else:
                df_original = read_csv_from_bytes(
                    file_bytes,
                    sep,
                    skip_header_rows,
                    header,
                    dtypes,
                    num_columns,
                    na_values,
                )

        column_names = [col[0] for col in schema]
        df_original.columns = column_names
        df = df_original.astype(
            {col_name: dtype_mapping[col_type] for col_name, col_type in schema}
        )

        logger.info("Successfully downloaded and read CSV.")
        return df

    except Exception as e:
        logger.exception(f"Error occurred: {e}")
        raise


def process_geo_admin_1_codes():
    table_name = "geo_admin_1_codes"
    url = "https://www.geonames.org/premiumdata/latest/admin1CodesASCII.txt"
    schema = [
        ["stateprovince_code", "string"],
        ["stateprovince_name", "string"],
        ["stateprovince_name_ascii", "string"],
        ["stateprovince_geoname_id", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_admin_2_codes():
    table_name = "geo_admin_2_codes"
    url = "https://www.geonames.org/premiumdata/latest/admin2Codes.txt"
    schema = [
        ["admin2_code", "string"],
        ["admin2_name", "string"],
        ["admin2_name_ascii", "string"],
        ["admin2_geoname_id", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_admincode_5():
    table_name = "geo_admin5_code"
    url = "https://www.geonames.org/premiumdata/latest/adminCode5.zip"
    schema = [
        ["admin5_geoname_id", "string"],
        ["admin5_code", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_all_countries():
    table_name = "geo_all_countries"
    url = "https://www.geonames.org/premiumdata/latest/allCountries.zip"
    schema = [
        ["geoname_id", "string"],
        ["name", "string"],
        ["asciiname", "string"],
        ["alternate_names", "string"],
        ["latitude", "string"],
        ["longitude", "string"],
        ["feature_class", "string"],
        ["feature_code", "string"],
        ["country_code", "string"],
        ["cc2", "string"],
        ["admin_1_code", "string"],
        ["admin_2_code", "string"],
        ["admin_3_code", "string"],
        ["admin_4_code", "string"],
        ["population", "string"],
        ["elevation", "string"],
        ["dem", "string"],
        ["timezone", "string"],
        ["modification_date", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_all_countries_deleted():
    table_name = "geo_all_countries_deleted"
    url = "https://www.geonames.org/premiumdata/latest/deletes.txt"
    schema = [
        ["geoname_id", "string"],
        ["name", "string"],
        ["comment", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "append",
        schema,
    )


def process_geo_all_countries_modified():
    table_name = "geo_all_countries_modified"
    url = "https://www.geonames.org/premiumdata/latest/modifications.zip"
    schema = [
        ["geoname_id", "integer"],
        ["name", "string"],
        ["name_ascii", "string"],
        ["alternate_names", "string"],
        ["latitude", "float"],
        ["longitude", "float"],
        ["feature_class", "string"],
        ["feature_code", "string"],
        ["country_code", "string"],
        ["alternate_country_codes", "string"],
        ["admin_1_code", "string"],
        ["admin_2_code", "string"],
        ["admin_3_code", "string"],
        ["admin_4_code", "string"],
        ["population", "integer"],
        ["elevation", "float"],
        ["digital_elevation_model", "float"],
        ["timezone", "string"],
        ["modification_date", "date"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema)
    df["modification_date"] = pd.to_datetime(df["modification_date"]).dt.date
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_alternate_names_deleted():
    table_name = "geo_alternate_names_deleted"
    url = "https://www.geonames.org/premiumdata/latest/alternateNamesDeletes.txt"
    schema = [
        ["alternatename_id", "string"],
        ["alternatename_geoname_id", "string"],
        ["alternate_name", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_alternate_names_modified():
    table_name = "geo_alternate_names_modified"
    url = "https://www.geonames.org/premiumdata/latest/alternateNamesModifications.zip"
    schema = [
        ["alternatename_id", "string"],
        ["alternatename_geoname_id", "string"],
        ["iso_language", "string"],
        ["alternate_name", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    df["modification_date"] = date.today()
    schema = [
        ["alternatename_id", "string"],
        ["alternatename_geoname_id", "string"],
        ["iso_language", "string"],
        ["alternate_name", "string"],
        ["modification_date", "date"],
    ]
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_alternate_names_v_2():
    table_name = "geo_alternate_names_v_2"
    url = "https://www.geonames.org/premiumdata/latest/alternateNamesV2.zip"
    schema = [
        ["alternatename_id", "string"],
        ["alternatename_geoname_id", "string"],
        ["iso_language", "string"],
        ["alternate_name", "string"],
        ["is_preferred_name", "string"],
        ["is_short_name", "string"],
        ["is_colloquial", "string"],
        ["is_historic", "string"],
        ["alternatename_start_date", "string"],
        ["alternatename_end_date", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(
        url, schema, skip_header_rows=0, file_name_regex=r"^alternateNamesV2"
    )
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_country_info():
    table_name = "geo_country_info"
    url = "https://www.geonames.org/premiumdata/latest/countryInfo.txt"
    schema = [
        ["iso_code", "string"],
        ["iso3_code", "string"],
        ["iso_numeric_code", "integer"],
        ["fips_code", "string"],
        ["country_name", "string"],
        ["Capital", "string"],
        ["area_in_square_kilometers", "float"],
        ["Population", "integer"],
        ["Continent", "string"],
        ["top_level_domain", "string"],
        ["currency_code", "string"],
        ["currency_name", "string"],
        ["phone", "string"],
        ["Postal_Code_Format", "string"],
        ["Postal_Code_Regex", "string"],
        ["Languages", "string"],
        ["country_geoname_id", "integer"],
        ["neighbors", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=50)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_feature_codes():
    table_name = "geo_feature_codes"
    url = "https://www.geonames.org/premiumdata/latest/featureCodes_en.txt"
    schema = [
        ["feature_code_id", "string"],
        ["feature_code_name", "string"],
        ["feature_code_description", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_geoip_2_city_blocks_ipv6():
    table_name = "geo_geoip_2_city_blocks_ipv6"
    url = "https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2-City-CSV&suffix=zip"
    schema = [
        ["network", "string"],
        ["geoname_id", "integer"],
        ["registered_country_geoname_id", "integer"],
        ["represented_country_geoname_id", "integer"],
        ["is_anonymous_proxy", "integer"],
        ["is_satellite_provider", "integer"],
        ["postal_code", "string"],
        ["latitude", "float"],
        ["longitude", "float"],
        ["accuracy_radius", "integer"],
        ["is_anycast", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(
        url,
        schema,
        sep=",",
        file_name_regex="GeoIP2-City-CSV_\d{8}\/GeoIP2-City-Blocks-IPv6.csv",
    )
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_geoip_2_city_locations():
    table_name = "geo_geoip_2_city_locations"
    url = "https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2-City-CSV&suffix=zip"
    schema = [
        ["geoname_id", "integer"],
        ["locale_code", "string"],
        ["continent_code", "string"],
        ["continent_name", "string"],
        ["country_iso_code", "string"],
        ["country_name", "string"],
        ["subdivision_1_iso_code", "string"],
        ["subdivision_1_name", "string"],
        ["subdivision_2_iso_code", "string"],
        ["subdivision_2_name", "string"],
        ["city_name", "string"],
        ["metro_code", "string"],
        ["time_zone", "string"],
        ["is_in_european_union", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(
        url,
        schema,
        sep=",",
        file_name_regex="GeoIP2-City-CSV_\d{8}\/GeoIP2-City-Locations-en\.csv",
    )
    schema = [
        ["geoname_id", "integer"],
        ["locale_code", "string"],
        ["continent_code", "string"],
        ["continent_name", "string"],
        ["country_iso_code", "string"],
        ["country_name", "string"],
        ["is_in_european_union", "integer"],
    ]
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_geoip_2_country_blocks_ipv6():
    table_name = "geo_geoip_2_country_blocks_ipv6"
    url = "https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2-Country-CSV&suffix=zip"
    schema = [
        ["network", "string"],
        ["geoname_id", "integer"],
        ["registered_country_geoname_id", "integer"],
        ["represented_country_geoname_id", "integer"],
        ["is_anonymous_proxy", "integer"],
        ["is_satellite_provider", "integer"],
        ["is_anycast", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(
        url,
        schema,
        sep=",",
        file_name_regex="GeoIP2-Country-CSV_\d{8}\/GeoIP2-Country-Blocks-IPv6.csv",
    )
    df = df[df["geoname_id"].notnull()]
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_geoip_2_country_locations():
    table_name = "geo_geoip_2_country_locations"
    url = "https://download.maxmind.com/app/geoip_download?edition_id=GeoIP2-Country-CSV&suffix=zip"
    schema = [
        ["geoname_id", "integer"],
        ["locale_code", "string"],
        ["continent_code", "string"],
        ["continent_name", "string"],
        ["country_iso_code", "string"],
        ["country_name", "string"],
        ["is_in_european_union", "integer"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(
        url,
        schema,
        sep=",",
        file_name_regex="GeoIP2-Country-CSV_\d{8}\/GeoIP2-Country-Locations-en\.csv",
    )
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_hierarchy():
    table_name = "geo_hierarchy"
    url = "https://www.geonames.org/premiumdata/latest/hierarchy.zip"
    schema = [
        ["parent_geoname_id", "integer"],
        ["child_geoname_id", "integer"],
        ["hierarchy_type", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_iso_language_codes():
    table_name = "geo_iso_language_codes"
    url = "https://www.geonames.org/premiumdata/latest/iso-languagecodes.txt"
    schema = [
        ["iso_639_3", "string"],
        ["iso_639_2", "string"],
        ["iso_639_1", "string"],
        ["language_name", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema, skip_header_rows=0)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


def process_geo_time_zones():
    table_name = "geo_time_zones"
    url = "https://www.geonames.org/premiumdata/latest/timeZones.txt"
    schema = [
        ["country_code", "string"],
        ["time_zone_id", "string"],
        ["gmt_offset_jan_1", "string"],
        ["dst_offset_jan_1", "string"],
        ["raw_offset_independent_of_dst", "string"],
    ]
    logger.info(f"Processing {table_name}...")
    df = load_to_dataframe(url, schema)
    client.upload_from_dataframe(
        df,
        dataset_name,
        table_name,
        "overwrite",
        schema,
    )


@functions_framework.http
def process_geography_data():
    logger.info("Start processing geography data")
    setup_logging()
    process_geo_admin_1_codes()
    process_geo_admin_2_codes()
    process_geo_admincode_5()
    process_geo_all_countries()
    process_geo_all_countries_deleted()
    process_geo_all_countries_modified()
    process_geo_alternate_names_deleted()
    process_geo_alternate_names_modified()
    process_geo_alternate_names_v_2()
    process_geo_country_info()
    process_geo_geoip_2_city_blocks_ipv6()
    process_geo_geoip_2_city_locations()
    process_geo_geoip_2_country_blocks_ipv6()
    process_geo_geoip_2_country_locations()
    process_geo_hierarchy()
    process_geo_feature_codes()
    process_geo_iso_language_codes()
    process_geo_time_zones()
    logger.info("Processing geography data completed")
