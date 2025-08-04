# Notes on image value:
# When not define it defaults to "us-docker.pkg.dev/cloudrun/container/job:latest" 
# This is a placeholder image -- only used on initial creation
# after creation, use the full path to the image in the format "${local.region}-docker.pkg.dev/${project_id}/gcrj-artifacts/${job_name}:latest"

module "process_geography" {
  source   = "git::https://github.com/CruGlobal/cru-terraform-modules.git//gcp/cloudrun-job/scheduled-tasks?ref=v32.1.2"
  paused   = false
  name     = "process-geography"
  image    = "${local.region}-docker.pkg.dev/${local.project_id}/gcrj-artifacts/process-geography:latest"
  schedule = "0 2 10 * *" # 2am on the 10th of every month

  time_zone = "UTC"
  secrets   = ["GEONAMES_PASSWORD", "GEONAMES_USERNAME", "MAXMIND_LICENSE_KEY"]
  env_variables = {
    BIGQUERY_DATASET_NAME = "el_geography"
    BIGQUERY_PROJECT_NAME = "cru-data-warehouse-elt-prod"
    GOOGLE_CLOUD_PROJECT  = local.project_id
  }

  secret_managers = [
    "user:luis.rodriguez@cru.org",
    "user:matt.drees@cru.org",
    "group:dps-gcp-role-data-engineers@cru.org",
  ]

  project_id  = local.project_id
  region      = local.region
  cpu         = "4"
  memory      = "16Gi"
  timeout     = 3600 # number of seconds
  max_retries = 1
}

module "okta_sync" {
  source   = "git::https://github.com/CruGlobal/cru-terraform-modules.git//gcp/cloudrun-job/scheduled-tasks?ref=v32.1.2"
  paused   = false
  name     = "okta-sync"
  image    = "${local.region}-docker.pkg.dev/${local.project_id}/gcrj-artifacts/okta-sync:latest"
  schedule = "0 6 * * *" # 6am daily

  time_zone = "UTC"
  secrets   = ["OKTA_TOKEN", "DBT_TOKEN"]
  env_variables = {
    GOOGLE_CLOUD_PROJECT = local.project_id
  }

  secret_managers = [
    "user:luis.rodriguez@cru.org",
    "user:matt.drees@cru.org",
    "user:tony.guan@cru.org",
    "group:dps-gcp-role-data-engineers@cru.org",
  ]

  project_id  = local.project_id
  region      = local.region
  cpu         = "2"
  memory      = "4Gi"
  timeout     = 1800 # 30 minutes in seconds
  max_retries = 1
}

module "woo_sync" {
  source   = "git::https://github.com/CruGlobal/cru-terraform-modules.git//gcp/cloudrun-job/scheduled-tasks?ref=v32.1.2"
  paused   = false
  name     = "woo-sync"
  image    = "us-docker.pkg.dev/cloudrun/container/job:latest"
  schedule = "0 5 * * *" # 5am daily

  time_zone = "UTC"
  secrets   = ["OKTA_TOKEN", "DBT_TOKEN"]
  env_variables = {
    GOOGLE_CLOUD_PROJECT = local.project_id
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
  timeout     = 1800 # 30 minutes in seconds
  max_retries = 1
}