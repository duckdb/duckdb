//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column_data_scan_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

enum class ColumnDataAllocatorType { BUFFER_MANAGER_ALLOCATOR, IN_MEMORY_ALLOCATOR };

struct ChunkManagementState {
	unordered_map<idx_t, BufferHandle> handles;
};

struct ColumnDataAppendState {
	ChunkManagementState current_chunk_state;
	vector<UnifiedVectorFormat> vector_data;
};

struct ColumnDataScanState {
	ChunkManagementState current_chunk_state;
	idx_t segment_index;
	idx_t chunk_index;
	idx_t current_row_index;
	idx_t next_row_index;
	vector<column_t> column_ids;
};

struct ColumnDataParallelScanState {
	ColumnDataScanState scan_state;
	mutex lock;
};

struct ColumnDataLocalScanState {
	ChunkManagementState current_chunk_state;
	idx_t current_row_index;
};

class ColumnDataRow {
public:
	ColumnDataRow(DataChunk &chunk, idx_t row_index, idx_t base_index);

	DataChunk &chunk;
	idx_t row_index;
	idx_t base_index;

public:
	Value GetValue(idx_t column_index) const;
	idx_t RowIndex() const;
};

} // namespace duckdb
