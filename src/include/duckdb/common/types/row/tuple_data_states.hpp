//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class TupleDataPinProperties : uint8_t {
	INVALID,
	//! TODO
	KEEP_EVERYTHING_PINNED,
	//! TODO
	UNPIN_AFTER_DONE,
	//! TODO
	ALREADY_PINNED
};

struct TupleDataManagementState {
	unordered_map<uint32_t, BufferHandle> row_handles;
	unordered_map<uint32_t, BufferHandle> heap_handles;
	TupleDataPinProperties properties = TupleDataPinProperties::INVALID;
	Vector row_locations = Vector(LogicalType::POINTER);
	Vector heap_locations = Vector(LogicalType::POINTER);
	Vector heap_sizes = Vector(LogicalType::UBIGINT);
};

struct TupleDataAppendState {
	TupleDataManagementState chunk_state;
	vector<UnifiedVectorFormat> vector_data;
	vector<column_t> column_ids;
};

enum class TupleDataScanProperties : uint8_t {
	INVALID,
	//! Allow zero copy scans - this introduces a dependency on the resulting vector on the scan state of the tuple
	//! data collection, which means vectors might not be valid anymore after the next chunk is scanned.
	ALLOW_ZERO_COPY,
	//! Disallow zero-copy scans, always copying data into the target vector
	//! As a result, data scanned will be valid even after the tuple data collection is destroyed
	DISALLOW_ZERO_COPY
};

struct TupleDataScanState {
	TupleDataManagementState chunk_state;
	idx_t segment_index;
	idx_t chunk_index;
	TupleDataScanProperties properties = TupleDataScanProperties::INVALID;
	vector<column_t> column_ids;
};

struct TupleDataParallelScanState {
	TupleDataScanState scan_state;
	mutex lock;
};

struct TupleDataLocalScanState {
	TupleDataManagementState chunk_state;
	idx_t segment_index = DConstants::INVALID_INDEX;
};

} // namespace duckdb
