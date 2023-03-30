#pragma once

#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/cbuffer_manager.hpp"

namespace duckdb {

//! Allocator that doesn't allow anything, basically asserting that it's never used
struct BlockedAllocator {
	static data_ptr_t BlockedAllocate(PrivateAllocatorData *private_data, idx_t size) {
		throw NotImplementedException("This allocator can't allocate");
	}
	static void BlockedDeallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
		throw NotImplementedException("This allocator can't free");
	}
	static data_ptr_t BlockedRealloc(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
	                                 idx_t size) {
		throw NotImplementedException("This allocator can't resize");
	}
	static Allocator &Get() {
		static Allocator allocator(BlockedAllocator::BlockedAllocate, BlockedAllocator::BlockedDeallocate,
		                           BlockedAllocator::BlockedRealloc, nullptr);
		return allocator;
	}
};

//! FileBuffer class that holds borrowed memory
//! because the memory isn't owned by the file buffer, the destructor is a no-op
class ExternalFileBuffer : public FileBuffer {
public:
	ExternalFileBuffer(data_ptr_t buffer, uint64_t size)
	    : FileBuffer(BlockedAllocator::Get(), FileBufferType::EXTERNAL_BUFFER, 0) {
		this->internal_buffer = buffer;
		this->buffer = internal_buffer + Storage::BLOCK_HEADER_SIZE;
		this->internal_size = size;
		this->size = internal_size - Storage::BLOCK_HEADER_SIZE;
	}

	~ExternalFileBuffer() {
		buffer = nullptr;
		internal_buffer = nullptr;
		size = 0;
		internal_size = 0;
	}
};

} // namespace duckdb
