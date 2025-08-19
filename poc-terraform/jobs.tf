# Notes on image value:
# When not define it defaults to "us-docker.pkg.dev/cloudrun/container/job:latest" 
# This is a placeholder image -- only used on initial creation
# after creation, use the full path to the image in the format "${local.region}-docker.pkg.dev/${project_id}/gcrj-artifacts/${job_name}:latest"

module "woo_sync" {
  source   = "git::https://github.com/CruGlobal/cru-terraform-modules.git//gcp/cloudrun-job/scheduled-tasks?ref=v32.1.2"
  paused   = false
  name     = "woo-sync"
  image    = "${local.region}-docker.pkg.dev/${local.project_id}/gcrj-artifacts/woo-sync:latest"
  schedule = "0 0 1 1 *" 
  

  time_zone = "UTC"
  secrets   = ["API_CLIENT_ID", "API_CLIENT_SECRET"]
  env_variables = {
    GOOGLE_CLOUD_PROJECT = local.project_id
    FL_RLS_VALUE = "familylife_woo"
    FL_STORE_WID = "-2088561343579951637"
    CRU_RLS_VALUE = "cru_woo"
    CRU_STORE_WID = "-4889130622699552160"
    BIGQUERY_EXECUTION_PROJECT_NAME = "cru-data-orchestration-poc"
    BIGQUERY_DATASET_NAME = "cru-data-warehouse-elt-prod.el_woocommerce_api"
    FL_API_ORDERS = "https://shop.familylife.com/wp-json/wc/v3/orders"
    FL_API_PRODUCTS = "https://shop.familylife.com/wp-json/wc/v3/products"
    FL_API_REFUNDS = "https://shop.familylife.com/wp-json/wc/v3/refunds"
    CRU_API_ORDERS = "https://crustore.org/wp-json/wc/v3/orders"
    CRU_API_PRODUCTS = "https://crustore.org/wp-json/wc/v3/products"
    CRU_API_REFUNDS = "https://crustore.org/wp-json/wc/v3/refunds"
  }

  secret_managers = [
    "user:luis.rodriguez@cru.org",
    "user:matt.drees@cru.org",
    "user:chad.kline@cru.org",
    "group:dps-gcp-role-data-engineers@cru.org",
  ]

  project_id  = local.project_id
  region      = local.region
  cpu         = "2"
  memory      = "4Gi"
  timeout     = 900 # 15 minutes in seconds
  max_retries = 1
}