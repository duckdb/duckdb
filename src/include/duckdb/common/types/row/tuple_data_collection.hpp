//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_layout.hpp"

namespace duckdb {

class TupleDataAllocator;
struct TupleDataChunk;
struct TupleDataAppendState;
struct TupleDataManagementState;
struct TupleDataScatterFunction;
struct TupleDataGatherFunction;

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
	void InitializeAppend(TupleDataAppendState &append_state);
	//! Append a DataChunk to this TupleDataCollection using the specified append state
	void Append(TupleDataAppendState &append_state, DataChunk &new_chunk);
	//! TODO: combine / scan

private:
	//! TODO:
	void Initialize(ClientContext &context, vector<LogicalType> types, vector<AggregateObject> aggregates, bool align);
	//! TODO:
	static TupleDataScatterFunction GetScatterFunction(const TupleDataLayout &layout, idx_t col_idx);
	//! TODO:
	static TupleDataGatherFunction GetGatherFunction(const TupleDataLayout &layout, idx_t col_idx);
	//! TODO:
	void ComputeEntrySizes(TupleDataAppendState &append_state, DataChunk &chunk);

private:
	//! The TupleDataAllocator
	shared_ptr<TupleDataAllocator> allocator;
	//! The layout of the TupleDataCollection
	TupleDataLayout layout;
	//! The number of entries stored in the TupleDataCollection
	idx_t count;
	//! The data segments of the TupleDataCollection
	vector<TupleDataSegment> segments;
	//! The set of scatter functions
	vector<TupleDataScatterFunction> scatter_functions;
	//! The set of gather functions
	vector<TupleDataGatherFunction> gather_functions;
};

} // namespace duckdb
