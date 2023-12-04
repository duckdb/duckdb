//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column/column_data_scan_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

enum class ColumnDataAllocatorType : uint8_t {
	//! Use a buffer manager to allocate large chunks of memory that vectors then use
	BUFFER_MANAGER_ALLOCATOR,
	//! Use an in-memory allocator, allocating data for every chunk
	//! This causes the column data collection to allocate blocks that are not tied to a buffer manager
	IN_MEMORY_ALLOCATOR,
	//! Use a buffer manager to allocate vectors, but use a StringHeap for strings
	HYBRID
};

enum class ColumnDataScanProperties : uint8_t {
	INVALID,
	//! Allow zero copy scans - this introduces a dependency on the resulting vector on the scan state of the column
	//! data collection, which means vectors might not be valid anymore after the next chunk is scanned.
	ALLOW_ZERO_COPY,
	//! Disallow zero-copy scans, always copying data into the target vector
	//! As a result, data scanned will be valid even after the column data collection is destroyed
	DISALLOW_ZERO_COPY
};

struct ChunkManagementState {
	unordered_map<idx_t, BufferHandle> handles;
	ColumnDataScanProperties properties = ColumnDataScanProperties::INVALID;
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
	ColumnDataScanProperties properties;
	vector<column_t> column_ids;
};

struct ColumnDataParallelScanState {
	ColumnDataScanState scan_state;
	mutex lock;
};

struct ColumnDataLocalScanState {
	ChunkManagementState current_chunk_state;
	idx_t current_segment_index = DConstants::INVALID_INDEX;
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
