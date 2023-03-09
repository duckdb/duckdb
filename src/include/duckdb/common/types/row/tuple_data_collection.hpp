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
	//! Constructs a TupleDataCollection with the specified layout
	TupleDataCollection(BufferManager &buffer_manager, const TupleDataLayout &layout);
	//! Constructs a TupleDataCollection with the same (shared) allocator
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

	//! Initializes an Append state - useful for optimizing many appends made to the same tuple data collection
	void InitializeAppend(TupleDataAppendState &append_state,
	                      TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE);
	//! Initializes an Append state - useful for optimizing many appends made to the same tuple data collection
	void InitializeAppend(TupleDataAppendState &append_state, vector<column_t> column_ids,
	                      TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE);
	//! Initializes the Pin state of an Append state
	//! - useful for optimizing many appends made to the same tuple data collection
	void InitializeAppend(TupleDataPinState &pin_state,
	                      TupleDataPinProperties = TupleDataPinProperties::UNPIN_AFTER_DONE);
	//! Initializes the Chunk state of an Append state
	//! - useful for optimizing many appends made to the same tuple data collection
	void InitializeAppend(TupleDataChunkState &chunk_state, vector<column_t> column_ids);
	//! Append a DataChunk directly to this TupleDataCollection - calls InitializeAppend and Append internally
	void Append(DataChunk &new_chunk, const SelectionVector &sel = *FlatVector::IncrementalSelectionVector(),
	            idx_t append_count = DConstants::INVALID_INDEX);
	//! Append a DataChunk directly to this TupleDataCollection - calls InitializeAppend and Append internally
	void Append(DataChunk &new_chunk, vector<column_t> column_ids,
	            const SelectionVector &append_sel = *FlatVector::IncrementalSelectionVector(),
	            idx_t append_count = DConstants::INVALID_INDEX);
	//! Append a DataChunk to this TupleDataCollection using the specified append state
	void Append(TupleDataAppendState &append_state, DataChunk &new_chunk,
	            const SelectionVector &append_sel = *FlatVector::IncrementalSelectionVector(),
	            idx_t append_count = DConstants::INVALID_INDEX);
	//! Append a DataChunk to this TupleDataCollection using the specified pin and chunk states
	void Append(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, DataChunk &new_chunk,
	            const SelectionVector &append_sel = *FlatVector::IncrementalSelectionVector(),
	            idx_t append_count = DConstants::INVALID_INDEX);
	//! Creates a UnifiedVectorFormat in the given chunk state for the given DataChunk
	static void ToUnifiedFormat(TupleDataChunkState &chunk_state, DataChunk &new_chunk);
	//! Computes the heap sizes for the new DataChunk that will be appended
	static void ComputeHeapSizes(TupleDataChunkState &chunk_state, DataChunk &new_chunk,
	                             const SelectionVector &append_sel, const idx_t append_count);
	//! Computes the heap sizes for the specific Vector that will be appended
	static void ComputeHeapSizes(Vector &heap_sizes_v, Vector &source_v, UnifiedVectorFormat &source,
	                             const SelectionVector &append_sel, const idx_t append_count,
	                             const idx_t original_count);
	//! Builds out the buffer space for the specified chunk state
	void Build(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, idx_t append_offset, idx_t append_count);
	//! Scatters the given DataChunk to the rows in the specified append state
	void Scatter(TupleDataChunkState &chunk_state, DataChunk &new_chunk, const SelectionVector &append_sel,
	             const idx_t append_count) const;
	//! Scatters the given Vector to the given column id to the rows in the specified append state
	void Scatter(TupleDataChunkState &chunk_state, Vector &source, const column_t column_id,
	             const SelectionVector &append_sel, const idx_t append_count, const idx_t original_count) const;
	//! Copy rows from input to the built Chunk state
	void CopyRows(TupleDataChunkState &chunk_state, TupleDataChunkState &input, const SelectionVector &append_sel,
	              const idx_t append_count) const;
	//! Finalizes the pin state, releasing or storing blocks
	void FinalizePinState(TupleDataPinState &pin_state, TupleDataSegment &segment);
	//! Finalizes the pin state, releasing or storing blocks
	void FinalizePinState(TupleDataPinState &pin_state);
	//! Appends the other TupleDataCollection to this, destroying the other data collection
	void Combine(TupleDataCollection &other);

	//! Initializes a chunk with the correct types that can be used to call Append/Scan
	void InitializeChunk(DataChunk &chunk) const;
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
	//! Gathers a DataChunk from the TupleDataCollection, given the specific row locations (requires full pin)
	void Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count, DataChunk &result,
	            const SelectionVector &target_sel) const;
	//! Gathers a DataChunk (only the columns given by column_ids) from the TupleDataCollection,
	//! given the specific row locations (requires full pin)
	void Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
	            const vector<column_t> &column_ids, DataChunk &result, const SelectionVector &target_sel) const;
	//! Gathers a Vector (from the given column id) from the TupleDataCollection
	//! given the specific row locations (requires full pin)
	void Gather(Vector &row_locations, const SelectionVector &sel, const idx_t scan_count, const column_t column_id,
	            Vector &result, const SelectionVector &target_sel) const;

	//! TODO:
	void Pin();
	//! TODO:
	void Unpin();

	//! Converts this TupleDataCollection to a string representation
	string ToString();
	//! Prints the string representation of this TupleDataCollection
	void Print();

private:
	//! Initializes the TupleDataCollection (called by the constructors)
	void Initialize();
	//! Gets the scatter function for the given column index
	static TupleDataScatterFunction GetScatterFunction(const TupleDataLayout &layout, idx_t col_idx);
	//! Gets the gather function for the given column index
	static TupleDataGatherFunction GetGatherFunction(const TupleDataLayout &layout, idx_t col_idx);
	//! Get the next segment/chunk index for the scan
	bool NextScanIndex(TupleDataScanState &scan_state, idx_t &segment_index, idx_t &chunk_index) const;
	//! Scans the chunk at the given segment/chunk indices
	void ScanAtIndex(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, const vector<column_t> &column_ids,
	                 idx_t segment_index, idx_t chunk_index, DataChunk &result);

	//! Verify counts of the segments in this collection
	void Verify() const;

private:
	//! The layout of the TupleDataCollection
	const TupleDataLayout layout;
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
