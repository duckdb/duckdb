//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/table/scan_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "storage/table/column_segment.hpp"
#include "storage/storage_lock.hpp"
#include "storage/buffer/buffer_handle.hpp"

namespace duckdb {
class LocalTableStorage;
class Index;
class PersistentSegment;
class TransientSegment;

struct IndexScanState {
	vector<column_t> column_ids;

	IndexScanState(vector<column_t> column_ids) : column_ids(column_ids) {
	}
	virtual ~IndexScanState() {
	}
};

struct TransientScanState {
	//! The transient segment that is currently being scanned
	TransientSegment *transient;
	//! The vector index of the transient segment
	index_t vector_index;
	//! The primary buffer handle
	unique_ptr<BufferHandle> primary_handle;
	//! The set of pinned block handles for this scan
	unordered_map<block_id_t, unique_ptr<BufferHandle>> handles;
	//! The locks that are held during the scan, only used by the index scan
	vector<unique_ptr<StorageLockKey>> locks;
};

struct LocalScanState {
	LocalTableStorage *storage = nullptr;

	index_t chunk_index;
	index_t max_index;
	index_t last_chunk_count;

	sel_t sel_vector_data[STANDARD_VECTOR_SIZE];
};

struct TableScanState {
	virtual ~TableScanState() {
	}

	index_t current_persistent_row, max_persistent_row;
	index_t current_transient_row, max_transient_row;
	unique_ptr<TransientScanState[]> transient_states;
	index_t offset;
	sel_t sel_vector[STANDARD_VECTOR_SIZE];
	vector<column_t> column_ids;
	LocalScanState local_state;
};

struct CreateIndexScanState : public TableScanState {
	vector<unique_ptr<StorageLockKey>> locks;
	unique_ptr<std::lock_guard<std::mutex>> append_lock;
};

struct TableIndexScanState {
	Index *index;
	unique_ptr<IndexScanState> index_state;
	LocalScanState local_state;
	vector<column_t> column_ids;
};

}
