
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
