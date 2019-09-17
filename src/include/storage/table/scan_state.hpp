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

namespace duckdb {
class LocalTableStorage;

struct ColumnScanState {
	ColumnPointer pointer;
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

	index_t offset, current_row, max_row;
	sel_t sel_vector[STANDARD_VECTOR_SIZE];
	vector<column_t> column_ids;
	unique_ptr<ColumnScanState[]> column_scans;
	LocalScanState local_state;
};

struct IndexTableScanState : public TableScanState {
	vector<unique_ptr<StorageLockKey>> locks;
};

}
