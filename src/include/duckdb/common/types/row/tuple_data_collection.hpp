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
struct RowOperationsState;

typedef void (*tuple_data_scatter_function_t)(const Vector &source, const TupleDataVectorFormat &source_format,
                                              const SelectionVector &append_sel, const idx_t append_count,
                                              const TupleDataLayout &layout, const Vector &row_locations,
                                              Vector &heap_locations, const idx_t col_idx,
                                              const UnifiedVectorFormat &list_format,
                                              const vector<TupleDataScatterFunction> &child_functions);

struct TupleDataScatterFunction {
	tuple_data_scatter_function_t function;
	vector<TupleDataScatterFunction> child_functions;
};

typedef void (*tuple_data_gather_function_t)(const TupleDataLayout &layout, Vector &row_locations, const idx_t col_idx,
                                             const SelectionVector &scan_sel, const idx_t scan_count, Vector &target,
                                             const SelectionVector &target_sel, optional_ptr<Vector> list_vector,
                                             const vector<TupleDataGatherFunction> &child_functions);

struct TupleDataGatherFunction {
	tuple_data_gather_function_t function;
	vector<TupleDataGatherFunction> child_functions;
};

//! TupleDataCollection represents a set of buffer-managed data stored in row format
//! FIXME: rename to RowDataCollection after we phase it out
class TupleDataCollection {
	friend class TupleDataChunkIterator;
	friend class PartitionedTupleData;

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
	//! Unpins all held pins
	void Unpin();

	//! Gets the scatter function for the given type
	static TupleDataScatterFunction GetScatterFunction(const LogicalType &type, bool within_collection = false);
	//! Gets the gather function for the given type
	static TupleDataGatherFunction GetGatherFunction(const LogicalType &type);

	//! Initializes an Append state - useful for optimizing many appends made to the same tuple data collection
	void InitializeAppend(TupleDataAppendState &append_state,
	                      TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE);
	//! Initializes an Append state - useful for optimizing many appends made to the same tuple data collection
	void InitializeAppend(TupleDataAppendState &append_state, vector<column_t> column_ids,
	                      TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE);
	//! Initializes the Pin state of an Append state
	//! - Useful for optimizing many appends made to the same tuple data collection
	void InitializeAppend(TupleDataPinState &pin_state,
	                      TupleDataPinProperties = TupleDataPinProperties::UNPIN_AFTER_DONE);
	//! Initializes the Chunk state of an Append state
	//! - Useful for optimizing many appends made to the same tuple data collection
	void InitializeChunkState(TupleDataChunkState &chunk_state, vector<column_t> column_ids = {});
	//! Initializes the Chunk state of an Append state
	//! - Useful for optimizing many appends made to the same tuple data collection
	static void InitializeChunkState(TupleDataChunkState &chunk_state, const vector<LogicalType> &types,
	                                 vector<column_t> column_ids = {});
	//! Append a DataChunk directly to this TupleDataCollection - calls InitializeAppend and Append internally
	void Append(DataChunk &new_chunk, const SelectionVector &append_sel = *FlatVector::IncrementalSelectionVector(),
	            idx_t append_count = DConstants::INVALID_INDEX);
	//! Append a DataChunk directly to this TupleDataCollection - calls InitializeAppend and Append internally
	void Append(DataChunk &new_chunk, vector<column_t> column_ids,
	            const SelectionVector &append_sel = *FlatVector::IncrementalSelectionVector(),
	            const idx_t append_count = DConstants::INVALID_INDEX);
	//! Append a DataChunk to this TupleDataCollection using the specified Append state
	void Append(TupleDataAppendState &append_state, DataChunk &new_chunk,
	            const SelectionVector &append_sel = *FlatVector::IncrementalSelectionVector(),
	            const idx_t append_count = DConstants::INVALID_INDEX);
	//! Append a DataChunk to this TupleDataCollection using the specified pin and Chunk states
	void Append(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, DataChunk &new_chunk,
	            const SelectionVector &append_sel = *FlatVector::IncrementalSelectionVector(),
	            const idx_t append_count = DConstants::INVALID_INDEX);
	//! Append a DataChunk to this TupleDataCollection using the specified pin and Chunk states
	//! - ToUnifiedFormat has already been called
	void AppendUnified(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, DataChunk &new_chunk,
	                   const SelectionVector &append_sel = *FlatVector::IncrementalSelectionVector(),
	                   const idx_t append_count = DConstants::INVALID_INDEX);

	//! Creates a UnifiedVectorFormat in the given Chunk state for the given DataChunk
	static void ToUnifiedFormat(TupleDataChunkState &chunk_state, DataChunk &new_chunk);
	//! Gets the UnifiedVectorFormat from the Chunk state as an array
	static void GetVectorData(const TupleDataChunkState &chunk_state, UnifiedVectorFormat result[]);
	//! Resets the cached cache vectors (used for ARRAY/LIST casts)
	static void ResetCachedCastVectors(TupleDataChunkState &chunk_state, const vector<column_t> &column_ids);
	//! Computes the heap sizes for the new DataChunk that will be appended
	static void ComputeHeapSizes(TupleDataChunkState &chunk_state, const DataChunk &new_chunk,
	                             const SelectionVector &append_sel, const idx_t append_count);

	//! Builds out the buffer space for the specified Chunk state
	void Build(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, const idx_t append_offset,
	           const idx_t append_count);
	//! Scatters the given DataChunk to the rows in the specified Chunk state
	void Scatter(TupleDataChunkState &chunk_state, const DataChunk &new_chunk, const SelectionVector &append_sel,
	             const idx_t append_count) const;
	//! Scatters the given Vector to the given column id to the rows in the specified Chunk state
	void Scatter(TupleDataChunkState &chunk_state, const Vector &source, const column_t column_id,
	             const SelectionVector &append_sel, const idx_t append_count) const;
	//! Copy rows from input to the built Chunk state
	void CopyRows(TupleDataChunkState &chunk_state, TupleDataChunkState &input, const SelectionVector &append_sel,
	              const idx_t append_count) const;

	//! Finalizes the Pin state, releasing or storing blocks
	void FinalizePinState(TupleDataPinState &pin_state, TupleDataSegment &segment);
	//! Finalizes the Pin state, releasing or storing blocks
	void FinalizePinState(TupleDataPinState &pin_state);

	//! Appends the other TupleDataCollection to this, destroying the other data collection
	void Combine(TupleDataCollection &other);
	//! Appends the other TupleDataCollection to this, destroying the other data collection
	void Combine(unique_ptr<TupleDataCollection> other);
	//! Resets the TupleDataCollection, clearing all data
	void Reset();

	//! Initializes a chunk with the correct types that can be used to call Append/Scan
	void InitializeChunk(DataChunk &chunk) const;
	//! Initializes a chunk with the correct types that can be used to call Append/Scan for the given columns
	void InitializeChunk(DataChunk &chunk, const vector<column_t> &columns) const;
	//! Initializes a chunk with the correct types for a given scan state
	void InitializeScanChunk(TupleDataScanState &state, DataChunk &chunk) const;
	//! Initializes a Scan state for scanning all columns
	void InitializeScan(TupleDataScanState &state,
	                    TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE) const;
	//! Initializes a Scan state for scanning a subset of the columns
	void InitializeScan(TupleDataScanState &state, vector<column_t> column_ids,
	                    TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE) const;
	//! Initialize a parallel scan over the tuple data collection over all columns
	void InitializeScan(TupleDataParallelScanState &state,
	                    TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE) const;
	//! Initialize a parallel scan over the tuple data collection over a subset of the columns
	void InitializeScan(TupleDataParallelScanState &gstate, vector<column_t> column_ids,
	                    TupleDataPinProperties properties = TupleDataPinProperties::UNPIN_AFTER_DONE) const;
	//! Scans a DataChunk from the TupleDataCollection
	bool Scan(TupleDataScanState &state, DataChunk &result);
	//! Scans a DataChunk from the TupleDataCollection
	bool Scan(TupleDataParallelScanState &gstate, TupleDataLocalScanState &lstate, DataChunk &result);
	//! Whether the last scan has been completed on this TupleDataCollection
	bool ScanComplete(const TupleDataScanState &state) const;

	//! Gathers a DataChunk from the TupleDataCollection, given the specific row locations (requires full pin)
	void Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count, DataChunk &result,
	            const SelectionVector &target_sel, vector<unique_ptr<Vector>> &cached_cast_vectors) const;
	//! Gathers a DataChunk (only the columns given by column_ids) from the TupleDataCollection,
	//! given the specific row locations (requires full pin)
	void Gather(Vector &row_locations, const SelectionVector &scan_sel, const idx_t scan_count,
	            const vector<column_t> &column_ids, DataChunk &result, const SelectionVector &target_sel,
	            vector<unique_ptr<Vector>> &cached_cast_vectors) const;
	//! Gathers a Vector (from the given column id) from the TupleDataCollection
	//! given the specific row locations (requires full pin)
	void Gather(Vector &row_locations, const SelectionVector &sel, const idx_t scan_count, const column_t column_id,
	            Vector &result, const SelectionVector &target_sel, optional_ptr<Vector> cached_cast_vector) const;

	//! Converts this TupleDataCollection to a string representation
	string ToString();
	//! Prints the string representation of this TupleDataCollection
	void Print();

	//! Verify that all blocks are pinned
	void VerifyEverythingPinned() const;

private:
	//! Initializes the TupleDataCollection (called by the constructor)
	void Initialize();
	//! Gets all column ids
	void GetAllColumnIDs(vector<column_t> &column_ids);
	//! Adds a segment to this TupleDataCollection
	void AddSegment(TupleDataSegment &&segment);

	//! Computes the heap sizes for the specific Vector that will be appended
	static void ComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v, TupleDataVectorFormat &source,
	                             const SelectionVector &append_sel, const idx_t append_count);
	//! Computes the heap sizes for the specific Vector that will be appended (within a list)
	static void WithinCollectionComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
	                                             TupleDataVectorFormat &source_format,
	                                             const SelectionVector &append_sel, const idx_t append_count,
	                                             const UnifiedVectorFormat &list_data);
	//! Computes the heap sizes for the fixed-size type Vector that will be appended (within a list)
	static void ComputeFixedWithinCollectionHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
	                                                  TupleDataVectorFormat &source_format,
	                                                  const SelectionVector &append_sel, const idx_t append_count,
	                                                  const UnifiedVectorFormat &list_data);
	//! Computes the heap sizes for the string Vector that will be appended (within a list)
	static void StringWithinCollectionComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
	                                                   TupleDataVectorFormat &source_format,
	                                                   const SelectionVector &append_sel, const idx_t append_count,
	                                                   const UnifiedVectorFormat &list_data);
	//! Computes the heap sizes for the struct Vector that will be appended (within a list)
	static void StructWithinCollectionComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
	                                                   TupleDataVectorFormat &source_format,
	                                                   const SelectionVector &append_sel, const idx_t append_count,
	                                                   const UnifiedVectorFormat &list_data);
	//! Computes the heap sizes for the list Vector that will be appended (within a list)
	static void CollectionWithinCollectionComputeHeapSizes(Vector &heap_sizes_v, const Vector &source_v,
	                                                       TupleDataVectorFormat &source_format,
	                                                       const SelectionVector &append_sel, const idx_t append_count,
	                                                       const UnifiedVectorFormat &list_data);

	//! Get the next segment/chunk index for the scan
	bool NextScanIndex(TupleDataScanState &scan_state, idx_t &segment_index, idx_t &chunk_index);
	//! Scans the chunk at the given segment/chunk indices
	void ScanAtIndex(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, const vector<column_t> &column_ids,
	                 idx_t segment_index, idx_t chunk_index, DataChunk &result);

	//! Verify count/data size of this collection
	void Verify() const;

private:
	//! The layout of the TupleDataCollection
	const TupleDataLayout layout;
	//! The TupleDataAllocator
	shared_ptr<TupleDataAllocator> allocator;
	//! The number of entries stored in the TupleDataCollection
	idx_t count;
	//! The size (in bytes) of this TupleDataCollection
	idx_t data_size;
	//! The data segments of the TupleDataCollection
	unsafe_vector<TupleDataSegment> segments;
	//! The set of scatter functions
	vector<TupleDataScatterFunction> scatter_functions;
	//! The set of gather functions
	vector<TupleDataGatherFunction> gather_functions;
};

} // namespace duckdb
