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
class PersistentSegment;
class TransientSegment;

struct UncompressedSegmentScanState {
	//! The shared lock that is held by the scan
	unique_ptr<StorageLockKey> lock;
	//! The handle to the current buffer that is held by the scan
	unique_ptr<ManagedBufferHandle> handle;
};

struct TransientScanState {
	//! The transient segment that is currently being scanned
	TransientSegment *transient;
	//! The vector index of the transient segment
	index_t vector_index;
	//! The scan state of the uncompressed segment
	UncompressedSegmentScanState state;
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

struct IndexTableScanState : public TableScanState {
	vector<unique_ptr<StorageLockKey>> locks;
};

}
