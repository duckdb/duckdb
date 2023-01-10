//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_block.hpp"

namespace duckdb {

class RowLayout;
struct TupleDataAppendState;
struct TupleDataManagementState;

//! TupleDataCollection represents a set of buffer-managed data stored in row format
class TupleDataCollection {
public:
	//! Constructs a buffer-managed tuple data collection with the specified types and aggregates
	TupleDataCollection(BufferManager &buffer_manager, vector<LogicalType> types, vector<AggregateObject> aggregates,
	                    bool align = true);
	//! Constructs a buffer-managed tuple data collection with the specified types
	TupleDataCollection(BufferManager &buffer_manager, vector<LogicalType> types, bool align = true);
	//! Constructs a buffer-managed tuple data collection with the specified aggregates
	TupleDataCollection(BufferManager &buffer_manager, vector<AggregateObject> aggregates, bool align = true);

public:
	//! The layout of the stored rows
	const RowLayout &GetLayout() const {
		return layout;
	}
	//! The number of rows stored in the tuple data collection
	const idx_t &Count() const {
		return count;
	}

	//! Initializes an Append state - useful for optimizing many appends made to the same tuple data collection
	void InitializeAppend(TupleDataAppendState &state);
	//! Append a DataChunk to this TupleDataCollection using the specified append state
	void Append(TupleDataAppendState &state, DataChunk &new_chunk, bool unify);

private:
	//! Allocate a row block, hold the pin in the state, and return its id
	uint32_t AllocateRowBlock(TupleDataAppendState &state);
	//! Allocate a heap block, hold the pin in the state, and return its id
	uint32_t AllocateHeapBlock(TupleDataAppendState &state, uint32_t capacity = Storage::BLOCK_SIZE);
	//! Pins the row block with the given id, and holds the pin in the state
	void PinRowBlock(TupleDataManagementState &state, uint32_t row_block_id);
	//! Pins the heap block with the given id, and holds the pin in the state
	void PinHeapBlock(TupleDataManagementState &state, uint32_t heap_block_id);
	//! Get a data pointer to the row block with given offset (in rows)
	data_ptr_t GetRowDataPointer(TupleDataManagementState &state, uint32_t row_block_id, uint32_t offset);
	//! Get a data pointer to the the block with given offset (in bytes)
	data_ptr_t GetHeapDataPointer(TupleDataManagementState &state, uint32_t heap_block_id, uint32_t offset);

	//! Reserves space for the data to be appended, and sets pointers where the rows will be written
	void Build(TupleDataAppendState &state, DataChunk &chunk);
	idx_t AppendToBlock(TupleDataAppendState &state, idx_t offset, idx_t append_count);
	void ComputeEntrySizes(TupleDataAppendState &state, DataChunk &chunk);

private:
	//! The buffer manager
	BufferManager &buffer_manager;
	//! The layout of the stored rows
	RowLayout layout;
	//! The number of rows stored in the tuple data collection
	idx_t count;
	//! Blocks storing the fixed-size rows
	vector<TupleRowDataBlock> row_blocks;
	//! Blocks storing the variable-size data of the fixed-size rows (e.g., string, list)
	vector<TupleHeapDataBlock> heap_blocks;
};

} // namespace duckdb
