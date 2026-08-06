"""Shared helpers imported by the workloads as `dot_shared.<module>`.

The workload directories are hyphenated (`fivetran-trigger`), so they are not
importable as packages; this one is underscored on purpose. Imports resolve
because the image runs with `PYTHONPATH=/app` (set on the Cloud Run
service/job by Terraform) and the buildpack copies the repo to `/app`.
"""
