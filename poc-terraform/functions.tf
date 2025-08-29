module "fivetran_trigger" {
  # TODO: use this when published; git@github.com:CruGlobal/cru-terraform-modules.git//gcp/cloudrun-function/scheduled-tasks?ref={{version-tag}}
  source = "git::https://github.com/CruGlobal/cru-terraform-modules.git//gcp/cloudrun-function/scheduled-tasks?ref=v30.14.4"

  name        = "fivetran-trigger"
  description = "A set of triggers to kick off Fivetran connection syncs for various systems"
  time_zone   = "UTC"
  schedule = {
    el_fivetran_logs : {
      cron : "0 0 1 1 *",
      argument = {
        connector_id = "pedestal_decision"
      }
    }
  }

  secrets = ["API_KEY", "API_SECRET"]

  secret_managers = [
    "user:luis.rodriguez@cru.org",
    "user:matt.drees@cru.org",
    "group:dps-gcp-role-data-engineers@cru.org",
  ]
  project_id = local.project_id
}

# End of fivetran_trigger module

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
