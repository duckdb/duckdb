//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/fixed_size_buffer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {

class FixedSizeAllocator;
class MetadataWriter;

//! A fixed-size buffer holds fixed-size segments of data. It lazily deserializes a buffer, if on-disk and not
//! yet in memory, and it only serializes dirty and non-written buffers to disk during
//! serialization.
class FixedSizeBuffer {
public:
	FixedSizeBuffer(const idx_t segment_count, const data_ptr_t memory_ptr)
	    : segment_count(segment_count), dirty(false), in_memory(true), on_disk(false), vacuum(false),
	      memory_ptr(memory_ptr) {
	}
	FixedSizeBuffer(const idx_t segment_count, const BlockPointer &block_ptr)
	    : segment_count(segment_count), dirty(false), in_memory(false), on_disk(true), vacuum(false),
	      block_ptr(block_ptr) {
	}

	//! The number of allocated segments
	idx_t segment_count;

	//! Flags to manage the memory of this buffer

	//! True: the in-memory buffer is no longer consistent with a (possibly existing)
	//! copy on disk
	bool dirty;
	//! True: the buffer is in memory
	bool in_memory;
	//! True: the buffer is serialized to disk
	bool on_disk;
	//! True: can be vacuumed after the vacuum operation
	bool vacuum;

	//! The buffer is in-memory, and memory_ptr points to its location
	data_ptr_t memory_ptr;
	//! Holds the block ID and offset of the buffer
	BlockPointer block_ptr;

public:
	//! Returns a pointer to the buffer in memory, and calls Deserialize, if
	//! the buffer is not in memory
	inline data_ptr_t Get(FixedSizeAllocator &fixed_size_allocator, const bool dirty_p = true) {
		if (!in_memory) {
			Deserialize(fixed_size_allocator);
		}
		if (dirty_p) {
			dirty = dirty_p;
		}
		return memory_ptr;
	}
	//! Serializes a buffer (if dirty or not on disk)
	void Serialize(FixedSizeAllocator &fixed_size_allocator, MetadataWriter &writer);
	//! Deserializes a buffer, if not in memory
	void Deserialize(FixedSizeAllocator &fixed_size_allocator);
};

} // namespace duckdb
