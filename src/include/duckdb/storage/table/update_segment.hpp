//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/update_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/common/types/string_heap.hpp"

namespace duckdb {
class ColumnData;
class DataTable;
class Vector;
struct UpdateInfo;
struct UpdateNode;

class UpdateSegment {
public:
	explicit UpdateSegment(ColumnData &column_data);
	~UpdateSegment();

	ColumnData &column_data;

public:
	bool HasUpdates() const;
	bool HasUncommittedUpdates(idx_t vector_index);
	bool HasUpdates(idx_t vector_index) const;
	bool HasUpdates(idx_t start_row_idx, idx_t end_row_idx);

	void FetchUpdates(TransactionData transaction, idx_t vector_index, Vector &result);
	void FetchCommitted(idx_t vector_index, Vector &result);
	void FetchCommittedRange(idx_t start_row, idx_t count, Vector &result);
	void Update(TransactionData transaction, idx_t column_index, Vector &update, row_t *ids, idx_t count,
	            Vector &base_data);
	void FetchRow(TransactionData transaction, idx_t row_id, Vector &result, idx_t result_idx);

	void RollbackUpdate(UpdateInfo &info);
	void CleanupUpdateInternal(const StorageLockKey &lock, UpdateInfo &info);
	void CleanupUpdate(UpdateInfo &info);

	unique_ptr<BaseStatistics> GetStatistics();
	StringHeap &GetStringHeap() {
		return heap;
	}

private:
	//! The lock for the update segment
	StorageLock lock;
	//! The root node (if any)
	unique_ptr<UpdateNode> root;
	//! Update statistics
	SegmentStatistics stats;
	//! Stats lock
	mutex stats_lock;
	//! Internal type size
	idx_t type_size;
	//! String heap, only used for strings
	StringHeap heap;

public:
	typedef void (*initialize_update_function_t)(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info,
	                                             Vector &update, const SelectionVector &sel);
	typedef void (*merge_update_function_t)(UpdateInfo *base_info, Vector &base_data, UpdateInfo *update_info,
	                                        Vector &update, row_t *ids, idx_t count, const SelectionVector &sel);
	typedef void (*fetch_update_function_t)(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info,
	                                        Vector &result);
	typedef void (*fetch_committed_function_t)(UpdateInfo *info, Vector &result);
	typedef void (*fetch_committed_range_function_t)(UpdateInfo *info, idx_t start, idx_t end, idx_t result_offset,
	                                                 Vector &result);
	typedef void (*fetch_row_function_t)(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info,
	                                     idx_t row_idx, Vector &result, idx_t result_idx);
	typedef void (*rollback_update_function_t)(UpdateInfo &base_info, UpdateInfo &rollback_info);
	typedef idx_t (*statistics_update_function_t)(UpdateSegment *segment, SegmentStatistics &stats, Vector &update,
	                                              idx_t count, SelectionVector &sel);

private:
	initialize_update_function_t initialize_update_function;
	merge_update_function_t merge_update_function;
	fetch_update_function_t fetch_update_function;
	fetch_committed_function_t fetch_committed_function;
	fetch_committed_range_function_t fetch_committed_range;
	fetch_row_function_t fetch_row_function;
	rollback_update_function_t rollback_update_function;
	statistics_update_function_t statistics_update_function;

private:
	void InitializeUpdateInfo(UpdateInfo &info, row_t *ids, const SelectionVector &sel, idx_t count, idx_t vector_index,
	                          idx_t vector_offset);
};

struct UpdateNodeData {
	unique_ptr<UpdateInfo> info;
	unsafe_unique_array<sel_t> tuples;
	unsafe_unique_array<data_t> tuple_data;
};

struct UpdateNode {
	unique_ptr<UpdateNodeData> info[Storage::ROW_GROUP_VECTOR_COUNT];
};

} // namespace duckdb
