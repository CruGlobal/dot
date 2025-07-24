terraform {

  #LOCAL ONLY
  backend "local" {
  }

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 6.7.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 6.7.0"
    }
  }
  required_version = ">= 1.11.3"
}

provider "google" {
  project               = local.project_id
  region                = local.region
  user_project_override = true
  default_labels        = local.labels
}
