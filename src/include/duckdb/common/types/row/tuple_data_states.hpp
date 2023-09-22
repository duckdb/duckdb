//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_states.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/perfect_map_set.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

enum class TupleDataPinProperties : uint8_t {
	INVALID,
	//! Keeps all passed blocks pinned while scanning/iterating over the chunks (for both reading/writing)
	KEEP_EVERYTHING_PINNED,
	//! Unpins blocks after they are done (for both reading/writing)
	UNPIN_AFTER_DONE,
	//! Destroys blocks after they are done (for reading only)
	DESTROY_AFTER_DONE,
	//! Assumes all blocks are already pinned (for reading only)
	ALREADY_PINNED
};

struct TupleDataPinState {
	perfect_map_t<BufferHandle> row_handles;
	perfect_map_t<BufferHandle> heap_handles;
	TupleDataPinProperties properties = TupleDataPinProperties::INVALID;
};

struct CombinedListData {
	UnifiedVectorFormat combined_data;
	list_entry_t combined_list_entries[STANDARD_VECTOR_SIZE];
	buffer_ptr<SelectionData> selection_data;
};

struct TupleDataVectorFormat {
	const SelectionVector *original_sel;
	SelectionVector original_owned_sel;

	UnifiedVectorFormat unified;
	vector<TupleDataVectorFormat> children;
	unique_ptr<CombinedListData> combined_list_data;
};

struct TupleDataChunkState {
	vector<TupleDataVectorFormat> vector_data;
	vector<column_t> column_ids;

	Vector row_locations = Vector(LogicalType::POINTER);
	Vector heap_locations = Vector(LogicalType::POINTER);
	Vector heap_sizes = Vector(LogicalType::UBIGINT);
};

struct TupleDataAppendState {
	TupleDataPinState pin_state;
	TupleDataChunkState chunk_state;
};

struct TupleDataScanState {
	TupleDataPinState pin_state;
	TupleDataChunkState chunk_state;
	idx_t segment_index = DConstants::INVALID_INDEX;
	idx_t chunk_index = DConstants::INVALID_INDEX;
};

struct TupleDataParallelScanState {
	TupleDataScanState scan_state;
	mutex lock;
};

using TupleDataLocalScanState = TupleDataScanState;

} // namespace duckdb
