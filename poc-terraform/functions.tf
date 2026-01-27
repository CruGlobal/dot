
module "dbt-triggers" {
  source      = "git::https://github.com/CruGlobal/cru-terraform-modules.git//gcp/cloudrun-function/scheduled-tasks?ref=v30.14.4"
  name        = "dbt-trigger"
  description = "A set of triggers to kick off dbt jobs"
  time_zone   = "UTC"
  schedule = {
    utilities : {
      cron : "0 0 1 1 *",
      argument = {
        "job_id" = "23366"
      }
    }
  }

  secrets = ["DBT_TOKEN"]

  secret_managers = [
    "user:luis.rodriguez@cru.org",
    "user:matt.drees@cru.org",
    "group:dps-gcp-role-data-engineers@cru.org",
  ]
  project_id = local.project_id
}

module "google-sheets-trigger" {
  source      = "git::https://github.com/CruGlobal/cru-terraform-modules.git//gcp/cloudrun-function/scheduled-tasks?ref=v30.14.4"
  name        = "google-sheets-trigger"
  description = "Checks Google Sheets for changes and triggers dbt jobs"
  time_zone   = "America/New_York"

  schedule = {
    dw_security_sheets : {
      cron : "0 17 * * 1-5", # M-F 5pm EST
      argument = {
        "sheets" = [
          { "id" = "1bJzA7_THeCd3oZiLAfktBqHSx9_bOdFEbWLMVkY47Mc", "name" = "Power BI RLS - Financial" },
          { "id" = "1f49EQA5B0GraOHHYjw3zYo6EKzuzbfoYWUSEynGNm4s", "name" = "BigQuery Table Access" },
          { "id" = "1Wm9rVCkn2r8u_p_BzABffAysfMbW24-XsX0rWoUQXMc", "name" = "Power BI RLS - Other" }
        ],
        "dbt_job_id"       = "920201",
        "include_weekends" = false
      }
    }
  }

  secrets = [] # No secrets needed - uses default SA credentials

  secret_managers = [
    "user:luis.rodriguez@cru.org",
    "user:matt.drees@cru.org",
    "group:dps-gcp-role-data-engineers@cru.org",
  ]
  project_id = local.project_id
}
