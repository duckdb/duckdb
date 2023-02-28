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
struct TupleDataChunkPart;
struct TupleDataSegment;
struct TupleDataManagementState;
struct TupleDataAppendState;

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

	//! Initializes an Append state - useful for optimizing many appends made to the same tuple data collection TODO
	void InitializeAppend(TupleDataAppendState &append_state);
	//! Initializes an Append state - useful for optimizing many appends made to the same tuple data collection TODO
	void InitializeAppend(TupleDataAppendState &append_state, vector<column_t> column_ids);
	//! Append a DataChunk directly to this TupleDataCollection - calls InitializeAppend and Append internally
	void Append(DataChunk &new_chunk, const SelectionVector &sel = *FlatVector::IncrementalSelectionVector());
	//! Append a DataChunk directly to this TupleDataCollection - calls InitializeAppend and Append internally TODO
	void Append(DataChunk &new_chunk, vector<column_t> column_ids,
	            const SelectionVector &sel = *FlatVector::IncrementalSelectionVector());
	//! Append a DataChunk to this TupleDataCollection using the specified append state
	void Append(TupleDataAppendState &append_state, DataChunk &new_chunk,
	            const SelectionVector &sel = *FlatVector::IncrementalSelectionVector());
	//! TODO:
	void Scatter(TupleDataAppendState &append_state, Vector &source, const column_t column_id, const idx_t append_count,
	             const SelectionVector &sel = *FlatVector::IncrementalSelectionVector());
	//! Appends the other TupleDataCollection to this, destroying the other data collection
	void Combine(TupleDataCollection &other);

	//! Initializes a chunk with the correct types that can be used to call Scan
	void InitializeScanChunk(DataChunk &chunk) const;
	//! Initializes a chunk with the correct types for a given scan state
	void InitializeScanChunk(TupleDataScanState &state, DataChunk &chunk) const;
	//! Initializes a Scan state for scanning all columns
	void InitializeScan(TupleDataScanState &state,
	                    TupleDataScanProperties properties = TupleDataScanProperties::ALLOW_ZERO_COPY) const;
	//! Initializes a Scan state for scanning a subset of the columns
	void InitializeScan(TupleDataScanState &state, vector<column_t> column_ids,
	                    TupleDataScanProperties properties = TupleDataScanProperties::ALLOW_ZERO_COPY) const;
	//! Initialize a parallel scan over the column data collection over all columns
	void InitializeScan(TupleDataParallelScanState &state,
	                    TupleDataScanProperties properties = TupleDataScanProperties::ALLOW_ZERO_COPY) const;
	//! Initialize a parallel scan over the column data collection over a subset of the columns
	void InitializeScan(TupleDataParallelScanState &gstate, vector<column_t> column_ids,
	                    TupleDataScanProperties properties = TupleDataScanProperties::ALLOW_ZERO_COPY) const;
	//! Scans a DataChunk from the TupleDataCollection
	bool Scan(TupleDataScanState &state, DataChunk &result) const;
	//! Scans a DataChunk from the TupleDataCollection
	bool Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate, DataChunk &result) const;
	//! Scans a DataChunk from the TupleDataCollection TODO
	void Gather(Vector &row_locations, const SelectionVector &sel, const vector<column_t> &column_ids,
	            const idx_t scan_count, DataChunk &result) const;
	//! TODO:
	void Gather(Vector &row_locations, const SelectionVector &sel, const column_t column_id, const idx_t scan_count,
	            Vector &result) const;

private:
	//! TODO:
	void Initialize(ClientContext &context, vector<LogicalType> types, vector<AggregateObject> aggregates, bool align);
	//! TODO:
	static TupleDataScatterFunction GetScatterFunction(const TupleDataLayout &layout, idx_t col_idx);
	//! TODO:
	static TupleDataGatherFunction GetGatherFunction(const TupleDataLayout &layout, idx_t col_idx);
	//! TODO:
	void ComputeEntrySizes(TupleDataAppendState &append_state, DataChunk &new_chunk, const SelectionVector &sel);
	//! TODO:
	bool NextScanIndex(TupleDataScanState &scan_state, idx_t &segment_index, idx_t &chunk_index) const;
	//!
	void ScanAtIndex(TupleDataManagementState &chunk_state, const vector<column_t> &column_ids, idx_t segment_index,
	                 idx_t chunk_index, DataChunk &result) const;

	//! Verify counts of the segments in this collection
	void Verify() const;

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
