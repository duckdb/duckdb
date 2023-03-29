#pragma once

#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/cbuffer_manager.hpp"

namespace duckdb {

class ExternalFileBuffer : public FileBuffer {
public:
	ExternalFileBuffer(data_ptr_t buffer, uint64_t size)
	    : FileBuffer(Allocator::DefaultAllocator(), FileBufferType::EXTERNAL_BUFFER, 0) {
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

	void Resize(uint64_t user_size) final override {
		throw NotImplementedException(
		    "This file buffer can not be resized, as the allocation is not owned by the buffer");
	}
};

} // namespace duckdb
