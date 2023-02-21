//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_allocator.hpp"
#include "duckdb/common/types/row/tuple_data_block.hpp"
#include "duckdb/common/types/row/tuple_data_layout.hpp"

namespace duckdb {

class RowLayout;
struct TupleDataAppendState;
struct TupleDataManagementState;

//! TupleDataCollection represents a set of buffer-managed data stored in row format
//! FIXME: rename to RowDataCollection after we phase it out
class TupleDataCollection {
public:
	//! Constructs a buffer-managed tuple data collection with the specified types and aggregates
	TupleDataCollection(ClientContext &context, vector<LogicalType> types, vector<AggregateObject> aggregates,
	                    bool align = true);
	//! Constructs a buffer-managed tuple data collection with the specified types
	TupleDataCollection(ClientContext &context, vector<LogicalType> types, bool align = true);
	//! Constructs a buffer-managed tuple data collection with the specified aggregates
	TupleDataCollection(ClientContext &context, vector<AggregateObject> aggregates, bool align = true);

public:
	//! The layout of the stored rows
	const TupleDataLayout &GetLayout() const {
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
	//! Reserves space for the data to be appended, and sets pointers where the rows will be written
	void Build(TupleDataAppendState &state, DataChunk &chunk);
	idx_t AppendToBlock(TupleDataAppendState &state, idx_t offset, idx_t append_count);
	void ComputeEntrySizes(TupleDataAppendState &state, DataChunk &chunk);

private:
	//! The allocator
	shared_ptr<TupleDataAllocator> allocator;
	//! The layout of the data
	TupleDataLayout layout;
	//! The number of rows stored in the tuple data collection
	idx_t count;
	//! Blocks storing the fixed-size rows
	vector<TupleRowDataSegment> row_blocks;
	//! Blocks storing the variable-size data of the fixed-size rows (e.g., string, list)
	vector<TupleHeapDataBlock> heap_blocks;
};

} // namespace duckdb
