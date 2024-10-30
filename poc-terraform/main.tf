module "fivetran-triggers" {
  # TODO: use this when published; git@github.com:CruGlobal/cru-terraform-modules.git//gcp/cloudrun-function/scheduled-tasks?ref={{version-tag}}
  source     = "../../cru-terraform-modules/gcp/cloudrun-function/scheduled-tasks"
  name = "fivetran-trigger"
  description = "A set of triggers to kick off Fivetran connection syncs for various systems"
  secrets = ["API_KEY", "API_SECRET"]
  time_zone  = "UTC"
  schedule = {
    john: {
      cron: "* * * * *",
      argument: {
        "name" = "John"
      }
    },
    sally: {
      cron: "*/5 * * * *",
      argument: {
        "name" = "Sally"
      }
    },
  }
  secret_managers = [
    "user:luis.rodriguez@cru.org",
    "user:matt.drees@cru.org",
    "group:dps-gcp-role-data-engineers@cru.org",
  ]
  project_id = local.project_id
}



