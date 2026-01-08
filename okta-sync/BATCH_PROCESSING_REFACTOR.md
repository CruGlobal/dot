# Okta Sync Batch Processing Refactor

## Overview
This refactor addresses critical memory consumption issues in the okta-sync task by implementing batch processing for data retrieval and upload operations.

## Problem Statement
The original implementation had two major memory bottlenecks:

1. **`get_data()` function (lines 223-269)**: Accumulated all paginated API responses into a single DataFrame using `pd.concat()` in a loop, causing memory to grow linearly with data size.

2. **`get_all_users()` function (lines 272-334)**: Similar issue - concatenated data for all group/app IDs and all their paginated results into one massive DataFrame before uploading.

For large datasets (e.g., thousands of groups with millions of members), this approach consumed excessive memory and could cause out-of-memory errors in Cloud Run.

## Solution

### New Functions

#### 1. `get_data_batch()` (Generator Function)
```python
def get_data_batch(endpoint, url, headers, params, batch_size=10)
```
- **Yields** DataFrames in batches instead of returning one large DataFrame
- Accumulates up to `batch_size` pages (default: 10) before yielding
- Clears memory after each yield
- Allows caller to process data incrementally

#### 2. `get_all_users_batch()` (Generator Function)
```python
def get_all_users_batch(endpoint, headers, params, ids, columns, id_batch_size=10)
```
- **Yields** DataFrames after processing every `id_batch_size` IDs (default: 10)
- Processes user data for each ID and accumulates until batch threshold
- Includes memory logging checkpoints
- Clears batch memory after yielding

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
- `batch_size=10` for API page batches in `get_data_batch()`
- `id_batch_size=10` for ID batches in `get_all_users_batch()`
- Can be adjusted based on memory constraints and data volume

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

## Notes

- Batch sizes can be tuned based on observed memory usage
- Generator pattern allows for streaming processing if needed in future
- CSV writes still use full DataFrame (for backward compatibility)
- Consider streaming uploads in future if memory is still constrained
