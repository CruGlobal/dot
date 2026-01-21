# Okta Sync Streaming Batch - Deployment Summary

## Executive Summary
This update converts okta-sync to stream batches to BigQuery and CSV checkpoints. It resolves the prior memory growth by avoiding full in-memory concatenation while preserving the existing temp dataset (`temp_okta`) -> target dataset (`el_okta`) swap.

## Branch Information
- **Branch**: `feature/okta-sync-batch-processing`
- **Base**: `main`
- **Status**: Ready for PR/merge

## Changes Summary

### Core Changes
- Batch generators yield DataFrames (`get_data_batch`, `get_all_users_batch`).
- `sync_data` and `sync_all_users` now upload each batch to BigQuery:
  - First batch uses `WRITE_TRUNCATE`.
  - Subsequent batches use `WRITE_APPEND`.
- After upload completes, the temp table is deduplicated in BigQuery (`SELECT DISTINCT *`).
- CSV output remains, but is streamed with append mode to keep resume capability without high RAM usage.

## Cloud Run Environment

**Current Production Configuration** (`cru-terraform/applications/data-warehouse/dot/prod/jobs.tf`):
```hcl
module "okta_sync" {
  cpu         = "4"
  memory      = "16Gi"
  timeout     = 64800  # 18 hours
  max_retries = 0
  schedule    = "0 17 * * 1-5"  # Weekdays 1pm EST
}
```

**Historical Memory Escalation:**
| Date | Memory | CPU | Reason |
|------|--------|-----|--------|
| Initial | 4Gi | 2 | Original allocation |
| July 2025 | 32Gi | 4 | OOM errors |
| Jan 2026 | 16Gi | 4 | Reduced after optimization attempts |

## Technical Details

### Streaming Strategy (New)
```python
for batch_df in get_data_batch(...):
    batch_df = match_schema(batch_df, schema_json)
    batch_df = batch_df.drop_duplicates()
    write_to_csv(batch_df, table_id, mode="a", include_header=first)
    upload_dataframe_to_bigquery(..., write_disposition="WRITE_APPEND")
```

### Deduplication (New)
After all batches are uploaded:
```sql
create or replace table {project}.{temp_okta}.{table} as
select distinct * from {project}.{temp_okta}.{table};
```

### Memory Characteristics
| Metric | Before | After |
|--------|--------|-------|
| Peak Memory | O(n) | O(b) |
| Scalability | Limited by RAM | Scales with batch size |

## Expected Impact
- **Memory**: Significant reduction (bounded by batch size).
- **Reliability**: Eliminates OOM failures during large syncs.
- **Cost**: Enables future reduction of Cloud Run memory if usage stabilizes below 16Gi.

## Deployment Plan

### Phase 1: Deploy
- Merge PR into `main`.
- Deploy Cloud Run job.

### Phase 2: Monitor
- Track memory usage via `log_memory_usage()` checkpoints.
- Verify record counts in BigQuery match prior runs.

### Phase 3: Optimize
- If memory is consistently low, reduce batch sizes or Cloud Run memory.

## Rollback Plan
- `use_batch_processing=False` restores legacy behavior.
- Previous version can be redeployed if needed.

## Monitoring Query
```sql
resource.type="cloud_run_job"
resource.labels.job_name="okta-sync"
jsonPayload.message=~"Memory Usage"
```

**Last Updated**: January 2026
