//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/fixed_size_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/partial_block_manager.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"

namespace duckdb {

class FixedSizeAllocator;
class MetadataWriter;

//! A fixed-size buffer holds fixed-size segments of data. It lazily deserializes a buffer, if on-disk and not
//! yet in memory, and it only serializes dirty and non-written buffers to disk during
//! serialization.
class FixedSizeBuffer {
public:
	//! Constructor for a new in-memory buffer
	explicit FixedSizeBuffer(PartialBlockManager &partial_block_manager);
	//! Constructor for deserializing buffer metadata from disk
	FixedSizeBuffer(PartialBlockManager &partial_block_manager, const idx_t segment_count, const idx_t allocated_memory,
	                const uint32_t max_offset, const BlockPointer &block_ptr);

	//! Partial block manager of the allocator
	PartialBlockManager &partial_block_manager;

	//! The number of allocated segments
	idx_t segment_count;

	//! The size of allocated memory in this buffer, skipping gaps
	idx_t allocated_memory;
	//! Maximum free bit in a bitmask plus one, so that max_offset * segment_size = allocated_size of this bitmask's
	//! buffer
	uint32_t max_offset;

	//! True: the in-memory buffer is no longer consistent with a (possibly existing) copy on disk
	bool dirty;
	//! True: can be vacuumed after the vacuum operation
	bool vacuum;
	//! True: the size of the buffer changed (new/free)
	bool size_changed;

	//! Partial block id and offset
	BlockPointer block_pointer;

public:
	//! Returns true, if the buffer is in-memory
	inline bool InMemory() const {
		return buffer_handle.IsValid();
	}
	//! Returns true, if the block is on-disk
	inline bool OnDisk() const {
		return block_pointer.IsValid();
	}
	//! Returns a pointer to the buffer in memory, and calls Deserialize, if the buffer is not in memory
	inline data_ptr_t Get(const bool dirty_p = true) {
		if (!InMemory()) {
			Pin();
		}
		if (dirty_p) {
			dirty = dirty_p;
		}
		return buffer_handle.Ptr() + block_pointer.offset;
	}
	//! Destroys the in-memory buffer and the on-disk block
	void Destroy();
	//! Serializes a buffer (if dirty or not on disk)
	void Serialize();
	//! Pin a buffer (if not in-memory)
	void Pin();

private:
	//! The buffer handle of the in-memory buffer
	BufferHandle buffer_handle;
	//! The block handle of the on-disk buffer
	shared_ptr<BlockHandle> block_handle;
};

} // namespace duckdb
