#pragma once

#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/cbuffer_manager.hpp"

namespace duckdb {

class ExternalFileBuffer : public FileBuffer {
public:
	ExternalFileBuffer(Allocator &allocator, uint64_t size)
	    : FileBuffer(allocator, FileBufferType::EXTERNAL_BUFFER, size), allocation(nullptr) {
	}

public:
	void Init() final override {
		FileBuffer::Init();
		allocation = nullptr;
	}

	data_ptr_t Buffer() const final override {
		// Use a callback to retrieve the actual allocation from an external buffer handle
		D_ASSERT(allocation);
		return allocation;
	}
	data_ptr_t ExternalBufferHandle() const {
		return buffer;
	}
	void SetAllocation(data_ptr_t allocation) {
		// FIXME: this is called when the readers count is 0, which can happen multiple times
		// that's why it only checks that the pointer isn't the same
		if (allocation != this->allocation) {
			D_ASSERT(!this->allocation);
			this->allocation = allocation;
		}
	}

private:
	//! The allocation associated with the buffer
	data_ptr_t allocation;
};

} // namespace duckdb
