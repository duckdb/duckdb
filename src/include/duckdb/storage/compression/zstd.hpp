//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/zstd.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {

class DictBuffer {
public:
	DictBuffer() : dict_buffer(nullptr), capacity(0), size(0) {
	}
	DictBuffer(uint32_t capacity) : dict_buffer(nullptr), capacity(capacity), size(capacity) {
		owned_buffer = make_unsafe_uniq_array_uninitialized<data_t>(capacity);
		dict_buffer = owned_buffer.get();
	}
	DictBuffer(void *buffer, uint32_t size) : dict_buffer(buffer), capacity(size), size(size) {
		D_ASSERT(dict_buffer);
	}
	DictBuffer(const DictBuffer &other) = delete;
	DictBuffer(DictBuffer &&other) = default;
	DictBuffer &operator=(DictBuffer &other) = delete;
	DictBuffer &operator=(DictBuffer &&other) = default;

public:
	operator bool() {
		return dict_buffer != nullptr;
	}
	void SetSize(uint32_t size_p) {
		D_ASSERT(size_p <= capacity);
		size = size_p;
	}
	uint32_t Size() const {
		return size;
	}
	uint32_t Capacity() const {
		return capacity;
	}
	void *Buffer() const {
		return dict_buffer;
	}

private:
	//! Optionally own the buffer, should be freed by the destructor
	unsafe_unique_array<data_t> owned_buffer = nullptr;
	void *dict_buffer = nullptr;
	uint32_t capacity = 0;
	uint32_t size = 0;
};

} // namespace duckdb
