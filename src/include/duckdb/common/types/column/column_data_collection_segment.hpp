//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/column/column_data_collection_segment.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/types/column/column_data_allocator.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector/flat_vector.hpp"

namespace duckdb {

//! Immutable, contiguous storage for the complete child data of large lists.
//! Instead of fragmenting a large list child over vector-sized block regions, a collection can store it as one
//! shared allocation. Scans hand out views of the allocation and appends that receive a complete view back
//! reference the allocation again, so a payload that passes through operators unchanged is never copied.
class ColumnDataListChildBuffer : public StandardVectorBuffer {
public:
	//! An owning buffer holding 'count' entries of a fixed-size type
	ColumnDataListChildBuffer(AllocatedData &&data, count_t count, idx_t type_size);
	//! A scan-local view of an owning buffer, kept alive through shared ownership
	ColumnDataListChildBuffer(buffer_ptr<VectorBuffer> owner, count_t count, idx_t type_size);

	//! The owning buffer ('self' must be the buffer_ptr holding this buffer)
	const buffer_ptr<VectorBuffer> &GetOwner(const buffer_ptr<VectorBuffer> &self) const {
		D_ASSERT(self.get() == this);
		return owner ? owner : self;
	}

private:
	//! The owning buffer referenced by a view, unset for owning buffers
	buffer_ptr<VectorBuffer> owner;
};

struct VectorChildIndex {
	explicit VectorChildIndex(idx_t index = DConstants::INVALID_INDEX) : index(index) {
	}

	idx_t index;

	bool IsValid() {
		return index != DConstants::INVALID_INDEX;
	}
};

struct VectorDataIndex {
	explicit VectorDataIndex(idx_t index = DConstants::INVALID_INDEX) : index(index) {
	}

	idx_t index;

	bool IsValid() {
		return index != DConstants::INVALID_INDEX;
	}
};

struct SwizzleMetaData {
	SwizzleMetaData(VectorDataIndex child_index_p, data_ptr_t ptr_p, uint16_t offset_p, uint16_t count_p)
	    : child_index(child_index_p), ptr(ptr_p), offset(offset_p), count(count_p) {
	}
	SwizzleMetaData(const SwizzleMetaData &other) noexcept
	    : SwizzleMetaData(other.child_index, other.ptr.load(std::memory_order_relaxed), other.offset, other.count) {
	}
	//! Index of block storing heap
	VectorDataIndex child_index;
	//! Last-known pointer
	atomic<data_ptr_t> ptr;
	//! Offset into the string_t vector
	uint16_t offset;
	//! Number of strings starting at 'offset' that have strings stored in the block with index 'child_index'
	uint16_t count;
};

struct VectorMetaData {
	//! Where the vector data lives
	uint32_t block_id;
	uint32_t offset;
	//! The number of entries present in this vector
	uint16_t count;
	//! Whether this entry references a shared list child buffer instead of block storage
	//! For such entries 'offset' indexes the segment's shared child buffers, and 'count' is set to
	//! STANDARD_VECTOR_SIZE so append paths treat the entry as full instead of writing into shared data
	bool is_shared_list_child = false;
	//! Meta data about string pointers
	vector<SwizzleMetaData> swizzle_data;

	//! Child data of this vector (used only for lists and structs)
	//! Note: child indices are stored with one layer of indirection
	//! The child_index here refers to the `child_indices` array in the ColumnDataCollectionSegment
	//! The entry in the child_indices array then refers to the actual `VectorMetaData` index
	//! In case of structs, the child_index refers to the FIRST child in the `child_indices` array
	//! Subsequent children are stored consecutively, i.e.
	//! first child: segment.child_indices[child_index + 0]
	//! nth child  : segment.child_indices[child_index + (n - 1)]
	VectorChildIndex child_index;
	//! Next vector entry (in case there is more data - used only in case of children of lists)
	VectorDataIndex next_data;
};

struct ChunkMetaData {
	//! The set of vectors of the chunk
	vector<VectorDataIndex> vector_data;
	//! The block ids referenced by the chunk
	unordered_set<uint32_t> block_ids;
	//! The number of entries in the chunk
	uint16_t count;
	//! Total allocation size for this chunk
	optional_idx allocation_size;
};

class ColumnDataCollectionSegment {
public:
	ColumnDataCollectionSegment(shared_ptr<ColumnDataAllocator> allocator, vector<LogicalType> types_p);

public:
	//! The allocator and allocation statistics
	shared_ptr<ColumnDataAllocator> allocator;
	idx_t total_allocated;
	idx_t last_chunk_total_allocated;

	//! The types of the chunks
	vector<LogicalType> types;
	//! The number of entries in the internal column data
	idx_t count;
	//! Set of chunk meta data
	vector<ChunkMetaData> chunk_data;
	//! Set of vector meta data
	vector<VectorMetaData> vector_data;
	//! The set of child indices
	vector<VectorDataIndex> child_indices;
	//! The string heap for the column data collection (only used for IN_MEMORY_ALLOCATOR)
	shared_ptr<StringHeap> heap;
	//! Shared list child buffers referenced by vector data entries
	vector<buffer_ptr<VectorBuffer>> shared_child_buffers;
	//! Allocation size of the distinct shared list child buffers
	idx_t shared_child_allocation_size = 0;

public:
	//! Reset this segment for reuse, clearing all metadata while keeping allocated buffers
	void Reset();
	void AllocateNewChunk();
	//! Allocate space for a vector of a specific type in the segment
	VectorDataIndex AllocateVector(const LogicalType &type, ChunkMetaData &chunk_data,
	                               ChunkManagementState *chunk_state = nullptr,
	                               VectorDataIndex prev_index = VectorDataIndex());
	//! Allocate space for a vector during append
	VectorDataIndex AllocateVector(const LogicalType &type, ChunkMetaData &chunk_data,
	                               ColumnDataAppendState &append_state, VectorDataIndex prev_index = VectorDataIndex());
	//! Allocate space for string data during append (BUFFER_MANAGER_ALLOCATOR only)
	VectorDataIndex AllocateStringHeap(idx_t size, ChunkMetaData &chunk_meta, ColumnDataAppendState &append_state,
	                                   VectorDataIndex prev_index = VectorDataIndex());

	void InitializeChunkState(idx_t chunk_index, ChunkManagementState &state);
	void ReadChunk(idx_t chunk_index, ChunkManagementState &state, DataChunk &chunk,
	               const vector<column_t> &column_ids);

	idx_t ReadVector(ChunkManagementState &state, VectorDataIndex vector_index, Vector &result);

	VectorDataIndex GetChildIndex(VectorChildIndex index, idx_t child_entry = 0);
	VectorChildIndex AddChildIndex(VectorDataIndex index);
	VectorChildIndex ReserveChildren(idx_t child_count);
	void SetChildIndex(VectorChildIndex base_idx, idx_t child_number, VectorDataIndex index);

	VectorMetaData &GetVectorData(VectorDataIndex index) {
		D_ASSERT(index.index < vector_data.size());
		return vector_data[index.index];
	}

	//! The number of entries in a vector data entry, resolving shared list children
	idx_t GetVectorCount(VectorDataIndex index);
	//! The shared buffer referenced by a shared list child entry
	const buffer_ptr<VectorBuffer> &GetSharedListChild(const VectorMetaData &vdata) const {
		D_ASSERT(vdata.is_shared_list_child && vdata.offset < shared_child_buffers.size());
		return shared_child_buffers[vdata.offset];
	}
	//! Add a vector data entry referencing a shared list child buffer
	VectorDataIndex AddSharedListChild(buffer_ptr<VectorBuffer> buffer, VectorDataIndex prev_index);

	idx_t ChunkCount() const;
	//! Get the total *used* size (not cached)
	idx_t SizeInBytes() const;
	//! Get the currently allocated size in bytes (cached)
	idx_t AllocationSize() const;
	//! Get the allocation size of the chunk
	idx_t GetChunkAllocationSize(idx_t chunk_index) const;

	void FetchChunk(idx_t chunk_idx, DataChunk &result);
	void FetchChunk(idx_t chunk_idx, DataChunk &result, const vector<column_t> &column_ids);

	void Verify();

	static idx_t GetDataSize(idx_t type_size);
	static validity_t *GetValidityPointerForWriting(data_ptr_t base_ptr, idx_t type_size);
	static validity_t *GetValidityPointer(data_ptr_t base_ptr, idx_t type_size, idx_t count);

private:
	idx_t ReadVectorInternal(ChunkManagementState &state, VectorDataIndex vector_index, Vector &result);
	VectorDataIndex AllocateVectorInternal(const LogicalType &type, ChunkMetaData &chunk_meta,
	                                       ChunkManagementState *chunk_state);
};

} // namespace duckdb
