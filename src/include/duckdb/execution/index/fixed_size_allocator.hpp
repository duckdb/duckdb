//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/fixed_size_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/execution/index/fixed_size_buffer.hpp"
#include "duckdb/execution/index/index_pointer.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/partial_block_manager.hpp"

namespace duckdb {

//! The FixedSizeAllocator provides pointers to fixed-size memory segments of pre-allocated memory buffers.
//! The pointers are IndexPointers, and the leftmost byte (metadata) must always be zero.
//! It is also possible to directly request a C++ pointer to the underlying segment of an index pointer.
class FixedSizeAllocator {
public:
	//! We can vacuum 10% or more of the total in-memory footprint
	static constexpr uint8_t VACUUM_THRESHOLD = 10;

public:
	//! Construct a new fixed-size allocator
	FixedSizeAllocator(const idx_t segment_size, BlockManager &block_manager);

	//! Block manager of the database instance
	BlockManager &block_manager;
	//! Buffer manager of the database instance
	BufferManager &buffer_manager;

public:
	//! Get a new IndexPointer to a segment, might cause a new buffer allocation
	IndexPointer New();
	//! Free the segment of the IndexPointer
	void Free(const IndexPointer ptr);
	//! Returns a pointer of type T to a segment. If dirty is false, then T should be a const class
	template <class T>
	inline T *Get(const IndexPointer ptr, const bool dirty = true) {
		return (T *)Get(ptr, dirty);
	}
	//! Returns the data_ptr_t to a segment, and sets the dirty flag of the buffer containing that segment
	inline data_ptr_t Get(const IndexPointer ptr, const bool dirty = true) {
		D_ASSERT(ptr.GetOffset() < available_segments_per_buffer);
		D_ASSERT(buffers.find(ptr.GetBufferId()) != buffers.end());
		auto &buffer = buffers.find(ptr.GetBufferId())->second;
		auto buffer_ptr = buffer.Get(dirty);
		return buffer_ptr + ptr.GetOffset() * segment_size + bitmask_offset;
	}

	//! Resets the allocator, e.g., during 'DELETE FROM table'
	void Reset();

	//! Returns the in-memory size in bytes
	idx_t GetInMemorySize() const;

	//! Returns the upper bound of the available buffer IDs, i.e., upper_bound > max_buffer_id
	idx_t GetUpperBoundBufferId() const;
	//! Merge another FixedSizeAllocator into this allocator. Both must have the same segment size
	void Merge(FixedSizeAllocator &other);

	//! Initialize a vacuum operation, and return true, if the allocator needs a vacuum
	bool InitializeVacuum();
	//! Finalize a vacuum operation by freeing all vacuumed buffers
	void FinalizeVacuum();
	//! Returns true, if an IndexPointer qualifies for a vacuum operation, and false otherwise
	inline bool NeedsVacuum(const IndexPointer ptr) const {
		if (vacuum_buffers.find(ptr.GetBufferId()) != vacuum_buffers.end()) {
			return true;
		}
		return false;
	}
	//! Vacuums an IndexPointer
	IndexPointer VacuumPointer(const IndexPointer ptr);

	//! Returns all FixedSizeAllocator information for serialization
	FixedSizeAllocatorInfo GetInfo() const;
	//! Serializes all in-memory buffers
	void SerializeBuffers(PartialBlockManager &partial_block_manager);
	//! Sets the allocation sizes and returns data to serialize each buffer
	vector<IndexBufferInfo> InitSerializationToWAL();
	//! Initialize a fixed-size allocator from allocator storage information
	void Init(const FixedSizeAllocatorInfo &info);
	//! Deserializes all metadata of older storage files
	void Deserialize(MetadataManager &metadata_manager, const BlockPointer &block_pointer);

private:
	//! Allocation size of one segment in a buffer
	//! We only need this value to calculate bitmask_count, bitmask_offset, and
	//! available_segments_per_buffer
	idx_t segment_size;

	//! Number of validity_t values in the bitmask
	idx_t bitmask_count;
	//! First starting byte of the payload (segments)
	idx_t bitmask_offset;
	//! Number of possible segment allocations per buffer
	idx_t available_segments_per_buffer;

	//! Total number of allocated segments in all buffers
	//! We can recalculate this by iterating over all buffers
	idx_t total_segment_count;

	//! Buffers containing the segments
	unordered_map<idx_t, FixedSizeBuffer> buffers;
	//! Buffers with free space
	unordered_set<idx_t> buffers_with_free_space;
	//! Buffers qualifying for a vacuum (helper field to allow for fast NeedsVacuum checks)
	unordered_set<idx_t> vacuum_buffers;

private:
	//! Returns an available buffer id
	idx_t GetAvailableBufferId() const;
};

} // namespace duckdb
