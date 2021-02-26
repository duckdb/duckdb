//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/update_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/morsel_info.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"

namespace duckdb {
class ColumnData;
class DataTable;
class Vector;
struct UpdateNode;

class UpdateSegment : public SegmentBase {
public:
	static constexpr const idx_t MORSEL_VECTOR_COUNT = MorselInfo::MORSEL_VECTOR_COUNT;
	static constexpr const idx_t MORSEL_SIZE = MorselInfo::MORSEL_SIZE;

	static constexpr const idx_t MORSEL_LAYER_COUNT = MorselInfo::MORSEL_LAYER_COUNT;
	static constexpr const idx_t MORSEL_LAYER_SIZE = MorselInfo::MORSEL_LAYER_SIZE;

public:
	UpdateSegment(ColumnData &column_data, idx_t start, idx_t count);
	~UpdateSegment();

	ColumnData &column_data;
public:
	bool HasUpdates();

	void MergeUpdates(Transaction &transaction, idx_t vector_index, Vector &result);
	void Update(Transaction &transaction, Vector &update, row_t *ids, idx_t count);
	SegmentStatistics &GetStatistics() {
		return stats;
	}
private:
	//! The lock for the update segment
	StorageLock lock;
	//! The root node (if any)
	unique_ptr<UpdateNode> root;
	//! Update statistics
	SegmentStatistics stats;
	//! Internal type size
	idx_t type_size;

public:
	typedef void (*initialize_update_function_t)(SegmentStatistics &stats, UpdateInfo *info, Vector &update);
	typedef void (*update_merge_function_t)(transaction_t start_time, transaction_t transaction_id, UpdateInfo *info, Vector &result);
private:
	initialize_update_function_t initialize_update_function;
	update_merge_function_t update_merge_function;

private:
	void InitializeUpdateInfo(UpdateInfo &info, row_t *ids, idx_t count, idx_t vector_index, idx_t vector_offset);
};

struct UpdateNodeData {
	unique_ptr<UpdateInfo> info;
	unique_ptr<sel_t[]> tuples;
	unique_ptr<data_t[]> tuple_data;
};

struct UpdateNode {
	unique_ptr<UpdateNodeData> info[UpdateSegment::MORSEL_VECTOR_COUNT];
};

} // namespace duckdb
