//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_collection.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/types/row/tuple_data_segment.hpp"
#include "duckdb/common/types/row/tuple_data_states.hpp"

namespace duckdb {

class TupleDataAllocator;
struct TupleDataScatterFunction;
struct TupleDataGatherFunction;

//! TupleDataCollection represents a set of buffer-managed data stored in row format
//! FIXME: rename to RowDataCollection after we phase it out
class TupleDataCollection {
	friend class TupleDataChunkIterator;

public:
	//! Constructs a buffer-managed tuple data collection with the specified layout
	TupleDataCollection(BufferManager &buffer_manager, TupleDataLayout layout);
	//! TODO:
	explicit TupleDataCollection(shared_ptr<TupleDataAllocator> allocator);

	~TupleDataCollection();

public:
	//! The layout of the stored rows
	const TupleDataLayout &GetLayout() const;
	//! The number of rows stored in the tuple data collection
	const idx_t &Count() const;
	//! The number of chunks stored in the tuple data collection
	idx_t ChunkCount() const;
	//! The size (in bytes) of the blocks held by this tuple data collection
	idx_t SizeInBytes() const;

	//! Initializes an Append state - useful for optimizing many appends made to the same tuple data collection TODO
	void InitializeAppend(TupleDataAppendState &append_state,
	                      TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE);
	//! Initializes an Append state - useful for optimizing many appends made to the same tuple data collection TODO
	void InitializeAppend(TupleDataAppendState &append_state, vector<column_t> column_ids,
	                      TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE);
	//! Append a DataChunk directly to this TupleDataCollection - calls InitializeAppend and Append internally
	void Append(DataChunk &new_chunk);
	//! Append a DataChunk directly to this TupleDataCollection - calls InitializeAppend and Append internally TODO
	void Append(DataChunk &new_chunk, vector<column_t> column_ids);
	//! Append a DataChunk to this TupleDataCollection using the specified append state
	void Append(TupleDataAppendState &append_state, DataChunk &new_chunk);
	//! TODO:
	void Scatter(TupleDataAppendState &append_state, Vector &source, const column_t column_id, const idx_t append_count,
	             const SelectionVector &sel);
	//! TODO:
	void FinalizeAppendState(TupleDataAppendState &append_state);
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
	bool Scan(TupleDataScanState &state, DataChunk &result);
	//! Scans a DataChunk from the TupleDataCollection
	bool Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate, DataChunk &result);
	//! TODO
	void Gather(Vector &row_locations, const SelectionVector &sel, const idx_t scan_count, DataChunk &result) const;
	//! Scans a DataChunk from the TupleDataCollection TODO
	void Gather(Vector &row_locations, const SelectionVector &sel, const vector<column_t> &column_ids,
	            const idx_t scan_count, DataChunk &result) const;
	//! TODO:
	void Gather(Vector &row_locations, const SelectionVector &sel, const column_t column_id, const idx_t scan_count,
	            Vector &result) const;

	//! TODO:
	void Pin();
	//! TODO:
	void Unpin();

	//! Converts this TupleDataCollection to a string representation
	string ToString();
	//! Prints the string representation of this TupleDataCollection
	void Print();

private:
	//! TODO:
	void Initialize();
	//! TODO:
	static TupleDataScatterFunction GetScatterFunction(const TupleDataLayout &layout, idx_t col_idx);
	//! TODO:
	static TupleDataGatherFunction GetGatherFunction(const TupleDataLayout &layout, idx_t col_idx);
	//! TODO:
	static void ComputeHeapSizes(TupleDataAppendState &append_state, DataChunk &new_chunk);
	//! TODO:
	static void ComputeHeapSizes(Vector &heap_sizes_v, Vector &source_v, UnifiedVectorFormat &source,
	                             const idx_t count);
	//! TODO:
	bool NextScanIndex(TupleDataScanState &scan_state, idx_t &segment_index, idx_t &chunk_index) const;
	//! TODO:
	void ScanAtIndex(TupleDataManagementState &chunk_state, const vector<column_t> &column_ids, idx_t segment_index,
	                 idx_t chunk_index, DataChunk &result);

	//! Verify counts of the segments in this collection
	void Verify() const;

private:
	//! The layout of the TupleDataCollection
	TupleDataLayout layout;
	//! The TupleDataAllocator
	shared_ptr<TupleDataAllocator> allocator;
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
