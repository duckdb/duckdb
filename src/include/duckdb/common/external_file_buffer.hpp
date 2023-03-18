#pragma once

#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/cbuffer_manager.hpp"

namespace duckdb {

class ExternalFileBuffer : public FileBuffer {
public:
	ExternalFileBuffer(Allocator &allocator, CBufferManagerConfig &config, uint64_t size)
	    : FileBuffer(allocator, FileBufferType::EXTERNAL_BUFFER, size), config(config) {
	}

public:
	data_ptr_t Buffer() const final override {
		// Use a callback to retrieve the actual allocation from an external buffer handle
		D_ASSERT(allocation);
		return allocation;
	}
	data_ptr_t ExternalBufferHandle() const {
		return buffer;
	}
	void SetAllocation(data_ptr_t allocation) {
		this->allocation = allocation;
	}

private:
	//! The allocation associated with the buffer
	data_ptr_t allocation;
	CBufferManagerConfig &config;
};

} // namespace duckdb
