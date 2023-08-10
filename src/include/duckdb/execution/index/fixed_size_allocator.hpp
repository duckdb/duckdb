//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/fixed_size_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/metadata/metadata_manager.hpp"
#include "duckdb/storage/metadata/metadata_writer.hpp"
#include "duckdb/execution/index/fixed_size_buffer.hpp"
#include "duckdb/execution/index/index_pointer.hpp"

namespace duckdb {

//! The FixedSizeAllocator provides pointers to fixed-size memory segments of pre-allocated memory buffers.
//! The pointers are IndexPointers, and the leftmost byte (metadata) must always be zero.
//! It is also possible to directly request a C++ pointer to the underlying segment of an index pointer.
class FixedSizeAllocator {
public:
	//! Fixed size of the buffers
	static constexpr idx_t BUFFER_SIZE = Storage::BLOCK_ALLOC_SIZE;
	//! We can vacuum 10% or more of the total in-memory footprint
	static constexpr uint8_t VACUUM_THRESHOLD = 10;

	//! Constants for fast offset calculations in the bitmask
	static constexpr idx_t BASE[] = {0x00000000FFFFFFFF, 0x0000FFFF, 0x00FF, 0x0F, 0x3, 0x1};
	static constexpr uint8_t SHIFT[] = {32, 16, 8, 4, 2, 1};

public:
	explicit FixedSizeAllocator(const idx_t segment_size, Allocator &allocator, MetadataManager &metadata_manager);
	~FixedSizeAllocator();

	//! Buffer manager of the database instance
	Allocator &allocator;
	//! Metadata manager for (de)serialization
	MetadataManager &metadata_manager;

public:
	//! Get a new IndexPointer to a segment, might cause a new buffer allocation
	IndexPointer New();
	//! Free the segment of the IndexPointer
	void Free(const IndexPointer ptr);
	//! Get the segment of the IndexPointer
	template <class T>
	inline T *Get(const IndexPointer ptr) {
		return (T *)Get(ptr);
	}

	//! Resets the allocator, e.g., during 'DELETE FROM table'
	void Reset();

	//! Returns the in-memory usage in bytes
	inline idx_t GetMemoryUsage() const;

	//! Returns the number of buffers
	inline idx_t GetBufferCount() const {
		return buffers.size();
	}
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

	//! Serializes all in-memory buffers and the metadata
	BlockPointer Serialize(MetadataWriter &writer);
	//! Deserializes all metadata
	void Deserialize(const BlockPointer &block_ptr);

private:
	//! Returns the data_ptr_t of an IndexPointer
	inline data_ptr_t Get(const IndexPointer ptr) {
		D_ASSERT(ptr.GetBufferId() < buffers.size());
		D_ASSERT(ptr.GetOffset() < available_segments_per_buffer);
		auto buffer_ptr = buffers[ptr.GetBufferId()].GetPtr(*this);
		return buffer_ptr + ptr.GetOffset() * segment_size + bitmask_offset;
	}
	//! Returns the first free offset in a bitmask
	uint32_t GetOffset(ValidityMask &mask, const idx_t segment_count);

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
	vector<FixedSizeBuffer> buffers;
	//! Buffers with free space
	unordered_set<idx_t> buffers_with_free_space;
	//! Buffers qualifying for a vacuum
	unordered_set<idx_t> vacuum_buffers;
};

} // namespace duckdb
