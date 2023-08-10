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
	    : segment_count(segment_count), dirty(false), in_memory(true), on_disk(false), memory_ptr(memory_ptr) {
	}
	FixedSizeBuffer(const idx_t segment_count, BlockPointer &block_ptr)
	    : segment_count(segment_count), dirty(false), in_memory(false), on_disk(true), block_ptr(block_ptr) {
	}
	idx_t segment_count;

	//! Flags to manage the memory of this buffer
	bool dirty;
	bool in_memory;
	bool on_disk;

	// TODO: is it worth combining these two into an IndexPointer to save space...?
	//! The buffer is in-memory, and memory_ptr points to its location
	data_ptr_t memory_ptr;
	//! Holds the block ID and offset of the buffer
	BlockPointer block_ptr;

public:
	data_ptr_t GetPtr(FixedSizeAllocator &fixed_size_allocator);
	void Serialize(FixedSizeAllocator &fixed_size_allocator, MetadataWriter &writer);
};

} // namespace duckdb
