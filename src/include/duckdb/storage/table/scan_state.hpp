//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/scan_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/storage/table/column_segment.hpp"

#include "duckdb/execution/adaptive_filter.hpp"

namespace duckdb {
class LocalTableStorage;
class Index;
class MorselInfo;
class UpdateSegment;
class PersistentSegment;
class TransientSegment;
class ValiditySegment;
struct TableFilterSet;

struct IndexScanState {
	virtual ~IndexScanState() {
	}
};

typedef unordered_map<block_id_t, unique_ptr<BufferHandle>> buffer_handle_set_t;

struct ColumnScanState {
	//! The column segment that is currently being scanned
	ColumnSegment *current;
	//! The vector index of the transient segment
	idx_t vector_index;
	//! The primary buffer handle
	unique_ptr<BufferHandle> primary_handle;
	//! Child states of the vector
	vector<ColumnScanState> child_states;
	//! Whether or not InitializeState has been called for this segment
	bool initialized = false;
	//! If this segment has already been checked for skipping puorposes
	bool segment_checked = false;
	//! The update segment of the current column
	UpdateSegment *updates;
	//! FIXME: all these vector offsets should be merged into a single row_index
	//! The vector index within the current update segment
	idx_t vector_index_updates;

public:
	//! Move on to the next vector in the scan
	void Next();
};

struct ColumnFetchState {
	//! The set of pinned block handles for this set of fetches
	buffer_handle_set_t handles;
	//! Any child states of the fetch
	vector<unique_ptr<ColumnFetchState>> child_states;
};

struct LocalScanState {
	~LocalScanState();

	void SetStorage(LocalTableStorage *storage);
	LocalTableStorage *GetStorage() {
		return storage;
	}

	idx_t chunk_index;
	idx_t max_index;
	idx_t last_chunk_count;
	TableFilterSet *table_filters;

private:
	LocalTableStorage *storage = nullptr;
};

class TableScanState {
public:
	TableScanState() {};
	idx_t current_row, max_row;
	idx_t base_row;
	unique_ptr<ColumnScanState[]> column_scans;
	idx_t column_count;
	TableFilterSet *table_filters = nullptr;
	unique_ptr<AdaptiveFilter> adaptive_filter;
	LocalScanState local_state;
	MorselInfo *version_info;

	//! Move to the next vector
	void NextVector();
};

class CreateIndexScanState : public TableScanState {
public:
	vector<unique_ptr<StorageLockKey>> locks;
	unique_lock<mutex> append_lock;
	unique_lock<mutex> delete_lock;
};

} // namespace duckdb
