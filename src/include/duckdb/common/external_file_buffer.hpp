#pragma once

#include "duckdb/common/file_buffer.hpp"
#include "duckdb/storage/cbuffer_manager.hpp"

namespace duckdb {

class ExternalFileBuffer : public FileBuffer {
public:
	ExternalFileBuffer(Allocator &allocator, CBufferManagerConfig config, uint64_t size)
	    : FileBuffer(allocator, FileBufferType::EXTERNAL_BUFFER, size) {
	}

public:
	data_ptr_t Buffer() const final override {
		// Use a callback to retrieve the actual allocation from an external buffer handle
		return (data_ptr_t)config.get_allocation_func(buffer);
	}
	data_ptr_t ExternalBufferHandle() const {
		return buffer;
	}

private:
	//! Contains the function to retrieve the buffer
	CBufferManagerConfig config;
};

} // namespace duckdb
