# Okta Sync Streaming Batch Refactor

## Overview
This refactor addresses memory growth in okta-sync by streaming data in batches to BigQuery and the local CSV checkpoint, instead of accumulating all results in memory.

## Cloud Run Context

**Current Production Configuration (Terraform: `cru-terraform/applications/data-warehouse/dot/prod/jobs.tf`):**
- **Memory**: 16Gi
- **CPU**: 4 cores
- **Timeout**: 64,800 seconds (~18 hours)
- **Schedule**: Weekdays at 1pm EST (5pm UTC)
- **Max Retries**: 0

**Historical Memory Escalation:**
- **Initial**: 4Gi memory, 2 CPU
- **July 2025**: Increased to **32Gi** due to OOM issues
- **January 2026**: Reduced to 16Gi after optimization attempts

## Problem Statement
The prior implementation loaded all pages and all IDs into a single DataFrame before upload. That resulted in peak memory scaling with total dataset size (O(n)), which caused OOM failures.

## Solution Summary

### Streaming Batch Uploads
- `get_data_batch()` yields DataFrames per page batch.
- `get_all_users_batch()` yields DataFrames per page batch across IDs.
- `sync_data()` and `sync_all_users()` upload each batch directly to BigQuery:
  - First batch uses `WRITE_TRUNCATE` to clear the temp table.
  - Subsequent batches use `WRITE_APPEND`.
- After all batches, the temp table is deduplicated in BigQuery using `SELECT DISTINCT *`.

### CSV Resume Checkpoint (Kept)
CSV output is still used to resume after interruptions, but now streams:
- First batch writes with `mode="w"` and headers.
- Later batches append with `mode="a"` and no header.

## Memory Characteristics

### Before
- **Peak memory**: O(n) (all records in RAM).

### After
- **Peak memory**: O(b) (current batch only).

## Usage

### Default (Batch Processing Enabled)
```python
sync_data("apps")
sync_all_users("group_members")
```

### Legacy Mode (No Batch Processing)
```python
sync_data("apps", use_batch_processing=False)
sync_all_users("group_members", use_batch_processing=False)
```

## Operational Notes
- BigQuery temp tables in `temp_okta` are the authoritative staging area.
- `replace_dataset_bigquery()` still swaps temp tables into `el_okta`.
- Deduplication happens in BigQuery, not in memory.
- CSVs remain as recovery points for interrupted runs.

## Monitoring
Watch Cloud Run logs for:
- Memory checkpoints from `log_memory_usage()`.
- Batch upload logs and dedupe completion.
