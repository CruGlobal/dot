module "fivetran_trigger" {
  source = "git::https://github.com/CruGlobal/cru-terraform-modules.git//gcp/cloudrun-function/scheduled-tasks?ref=v30.14.4"

  name        = "fivetran-trigger"
  description = "A set of triggers to kick off Fivetran connection syncs for various systems"

  time_zone = "UTC"
  schedule = {
    el_fivetran_logs_prod = {
      # Runs Daily Mon to Fri 6am UTC
      cron = "0 6 * * 1-5"
      argument = {
        "connector_id" = "pedestal_decision"
      }
    },
    el_fivetran_logs_stage = {
      # Runs Weekly Sunday at midnight UTC
      cron = "0 0 * * 0"
      argument = {
        "connector_id" = "grip_oblivious"
      }
    },
    el_netsuite_prod = {
      # Runs Daily 3am, 9am, 3pm EST
      cron = "0 8,14,20 * * *"
      argument = {
        "connector_id" = "smoothing_recognize"
      }
    },
    el_woocommerce_prod = {
      # Runs Daily 7am, 9am, 11am, 1pm, 3pm, 5pm 7pm EST
      cron = "0 0,12,14,16,18,20,22 * * *"
      argument = {
        "connector_id" = "unreached_challenging"
      }
    },
    el_familylife_salesforce_prod = {
      # Runs Daily 7am, 9am, 11am, 1pm, 3pm, 5pm 7pm EST
      cron = "0 0,12,14,16,18,20,22 * * *"
      argument = {
        "connector_id" = "implode_inquiry"
      }
    },
    el_crm_prod = {
      # Runs Daily 12am EST
      cron = "0 5 * * *"
      argument = {
        "connector_id" = "trifling_pitcher"
      }
    },
    el_infobase_prod = {
      # Runs Hourly at 00:00
      cron = "0 * * * *"
      argument = {
        "connector_id" = "smugness_flowing"
      }
    },
    el_github_prod = {
      # Runs Daily 7am & 7pm EST
      cron = "0 0,12 * * *"
      argument = {
        "connector_id" = "antagonism_strain"
      }
    },
    el_phire_oci_prod = {
      # Runs Daily 7am & 7pm EST
      cron = "0 0,12 * * *"
      argument = {
        "connector_id" = "freshly_caddy"
      }
    },
    el_psfin_oci_prod = {
      # Runs Daily 6am EST
      cron = "0 11 * * *"
      argument = {
        "connector_id" = "dearth_capably"
      }
    },
    el_helpscout_prod = {
      # Runs Daily 5am, 1pm EST
      cron = "0 10,18 * * *"
      argument = {
        "connector_id" = "wait_splendidly"
      }
    },
    el_global_registry_flat_prod = {
      # Runs Hourly at 00:00
      cron = "0 * * * *"
      argument = {
        "connector_id" = "freebee_tuberculosis"
      }
    },
    el_pshr_oci_prod = {
      # Runs Daily 6 pm EST
      cron = "0 23 * * *"
      argument = {
        "connector_id" = "supervision_narrowly"
      }
    },
    el_mpdx_prod = {
      # Runs every 6 hours
      cron = "0 */6 * * *"
      argument = {
        "connector_id" = "loft_unabashed"
      }
    },
    el_machine_learning_data_aem_site_data_prod = {
      # Runs every 10 minutes
      cron = "*/10 * * * *"
      argument = {
        "connector_id" = "unclothed_cheddar"
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

module "dbt-triggers" {
  source      = "git::https://github.com/CruGlobal/cru-terraform-modules.git//gcp/cloudrun-function/scheduled-tasks?ref=v30.14.4"
  name        = "dbt-trigger"
  description = "A set of triggers to kick off dbt jobs"
  time_zone   = "UTC"
  schedule = {
    doc_src_freshness : {
      cron : "0 0 1 1 *",
      argument = {
        "job_id" = "18120"
      }
    },
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



