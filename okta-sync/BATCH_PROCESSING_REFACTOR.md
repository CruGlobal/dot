# Okta Sync Batch Processing Refactor

## Overview
This refactor addresses critical memory consumption issues in the okta-sync task by implementing batch processing for data retrieval and upload operations.

## Cloud Run Environment Context

**Current Production Configuration (Terraform: `cru-terraform/applications/data-warehouse/dot/prod/jobs.tf`):**
- **Memory**: 16Gi (16 GB)
- **CPU**: 4 cores
- **Timeout**: 64,800 seconds (~18 hours)
- **Schedule**: Weekdays at 1pm EST (5pm UTC)
- **Max Retries**: 0
- **Image**: `us-central1-docker.pkg.dev/.../okta-sync:latest`

**Historical Memory Escalation:**
- **Initial**: 4Gi memory, 2 CPU
- **July 2025**: Increased to **32Gi** (8x increase!) due to OOM issues
- **January 2026**: Reduced to 16Gi after optimization attempts
- **Current Issue**: Still at 4x original allocation

This history demonstrates that memory consumption has been a **critical production issue**. The refactor aims to:
1. Reduce memory footprint significantly below 16Gi
2. Enable future data growth without infrastructure changes
3. Potentially allow reducing back to 8Gi or less

## Problem Statement
The original implementation had two major memory bottlenecks:

1. **`get_data()` function (lines 223-269)**: Accumulated all paginated API responses into a single DataFrame using `pd.concat()` in a loop, causing memory to grow linearly with data size.

2. **`get_all_users()` function (lines 272-334)**: Similar issue - concatenated data for all group/app IDs and all their paginated results into one massive DataFrame before uploading.

For large datasets (e.g., thousands of groups with millions of members), this approach consumed excessive memory and could cause out-of-memory errors in Cloud Run.

## Solution

### New Functions

#### 1. `get_data_batch()` (Generator Function)
```python
def get_data_batch(endpoint, url, headers, params, batch_size=50)
```
- **Yields** DataFrames in batches instead of returning one large DataFrame
- Accumulates up to `batch_size` pages (default: 50) before yielding
- Clears memory after each yield
- Allows caller to process data incrementally
- **Batch size rationale**: With 16Gi available, 50 pages (typically ~200 records/page = 10K records) is a reasonable batch that balances memory efficiency with processing speed

#### 2. `get_all_users_batch()` (Generator Function)
```python
def get_all_users_batch(endpoint, headers, params, ids, columns, id_batch_size=50)
```
- **Yields** DataFrames after processing every `id_batch_size` IDs (default: 50)
- Processes user data for each ID and accumulates until batch threshold
- Includes memory logging checkpoints
- Clears batch memory after yielding
- **Batch size rationale**: Processing 50 group/app IDs at once allows good throughput while keeping memory under control

### Updated Functions

#### 3. `sync_data(endpoint, use_batch_processing=True)`
- Added `use_batch_processing` parameter (default: `True`)
- When enabled:
  - Uses `get_data_batch()` to retrieve data in batches
  - Accumulates batches into a list
  - Combines batches only when ready to upload
  - Explicitly deletes DataFrames after upload to free memory
  - Logs memory usage at key checkpoints
- Original behavior preserved when `use_batch_processing=False`

#### 4. `sync_all_users(endpoint, use_batch_processing=True)`
- Added `use_batch_processing` parameter (default: `True`)
- When enabled:
  - Uses `get_all_users_batch()` generator
  - Processes each batch: applies schema, deduplicates
  - Accumulates processed batches
  - Combines and uploads at the end
  - Explicitly deletes DataFrames after upload
  - Logs memory usage between batches
- Original behavior preserved when `use_batch_processing=False`

## Key Improvements

### Memory Optimization
- **Batch Processing**: Data is processed in configurable chunks rather than all at once
- **Generator Pattern**: Uses Python generators to yield data incrementally
- **Explicit Cleanup**: `del` statements ensure DataFrames are freed after use
- **Memory Monitoring**: Added `log_memory_usage()` calls at critical points

### Configurable Batch Sizes
- `batch_size=50` for API page batches in `get_data_batch()`
- `id_batch_size=50` for ID batches in `get_all_users_batch()`
- Can be adjusted based on memory constraints and data volume
- **Tuned for 16Gi Cloud Run environment**: Batch sizes optimized for current production allocation

### Backward Compatibility
- Original functions (`get_data()`, `get_all_users()`) retained as deprecated
- New functions opt-in via `use_batch_processing` parameter
- Default behavior uses batch processing for immediate benefits

## Usage Examples

### Default (Batch Processing Enabled)
```python
# Automatically uses batch processing
sync_data("apps")
sync_all_users("group_members")
```

### Disable Batch Processing (Legacy Mode)
```python
# Uses original memory-intensive approach
sync_data("apps", use_batch_processing=False)
sync_all_users("group_members", use_batch_processing=False)
```

### Custom Batch Sizes
```python
# Smaller batches for memory-constrained environments
for batch in get_data_batch(endpoint, url, headers, params, batch_size=5):
    process_batch(batch)

# Process fewer IDs at once
for batch in get_all_users_batch(endpoint, headers, params, ids, columns, id_batch_size=5):
    process_batch(batch)
```

## Performance Characteristics

### Before (Original)
- **Memory**: O(n) where n = total records
- **Peak Memory**: All data loaded simultaneously
- **Risk**: Out-of-memory errors on large datasets

### After (Batch Processing)
- **Memory**: O(b) where b = batch size
- **Peak Memory**: Only current batch + accumulated small batches
- **Risk**: Significantly reduced, scales to much larger datasets

## Testing Recommendations

1. **Small Dataset Test**: Verify correctness with `batch_size=2` on small dataset
2. **Memory Monitoring**: Compare memory usage logs before/after refactor
3. **Large Dataset Test**: Run on production-scale data to confirm memory improvements
4. **Backward Compatibility**: Test with `use_batch_processing=False` to ensure original behavior works

## Migration Path

1. ✅ **Phase 1**: Deploy with batch processing enabled by default
2. **Phase 2**: Monitor memory usage in production for 1-2 runs
3. **Phase 3**: Remove deprecated functions if satisfied with performance
4. **Phase 4**: Make batch processing mandatory (remove flag)

## Memory Logging Points

The refactor adds memory logging at these checkpoints:
- After yielding each batch in `get_all_users_batch()`
- After combining batches in `sync_data()`
- After uploading to BigQuery in `sync_data()`
- After processing each batch in `sync_all_users()`
- After combining all batches in `sync_all_users()`
- After uploading to BigQuery in `sync_all_users()`

## Production Deployment Considerations

### Memory Monitoring
After deploying this refactor, monitor Cloud Run logs for:
- Process memory usage at checkpoints (logged via `log_memory_usage()`)
- Peak memory consumption during sync
- Time to completion (may be slightly longer due to batch processing overhead)

### Potential Infrastructure Optimization
If memory usage drops significantly (e.g., stays under 8Gi):
1. Consider reducing Cloud Run memory allocation in Terraform
2. Could reduce to 8Gi or even back to original 4Gi
3. Cost savings: ~$0.025/GB-hour × 8GB reduction × 18 hours/run × 5 runs/week

### If Memory Issues Persist
If still approaching 16Gi limits:
1. Reduce batch sizes to 25 or even 10
2. Consider implementing streaming uploads to BigQuery
3. Process different endpoints sequentially instead of batching them all in memory

## Notes

- Batch sizes (50) are optimized for 16Gi Cloud Run environment
- Can be tuned down (25, 10) if memory pressure observed
- Generator pattern allows for streaming processing if needed in future
- CSV writes still use full DataFrame (for backward compatibility with existing workflows)
- Consider streaming uploads in future if memory is still constrained despite batching
