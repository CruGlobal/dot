locals {
  identifier       = "dot"
  identifier_short = "dot"
  env              = "poc"

  project_id = "cru-data-orchestration-${local.env}"
  region     = "us-east4"
  domain     = "${local.identifier}-${local.env}.cru.org"

  tags = {
    name        = local.identifier
    project     = local.project_id
    owner       = "datahelp@cru.org"
    managed_by  = "local-terraform"
    application = "data-orchestration-tool"
  }
  labels = {
    name        = local.tags.name
    env         = local.env
    owner       = "datahelp_at_cru_org"
    application = local.tags.application
    managed_by  = local.tags.managed_by
  }
}
