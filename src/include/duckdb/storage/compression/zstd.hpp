//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/zstd.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string_util.hpp"

namespace duckdb {

class DictBuffer {
public:
	DictBuffer() : dict_buffer(nullptr), capacity(0), size(0) {
	}
	DictBuffer(idx_t capacity) : dict_buffer(nullptr), capacity(capacity), size(capacity) {
		dict_buffer = malloc(capacity);
	}
	DictBuffer(void *buffer, idx_t size) : dict_buffer(buffer), capacity(size), size(size) {
		D_ASSERT(dict_buffer);
	}
	~DictBuffer() {
		free(dict_buffer);
	}
	DictBuffer(const DictBuffer &other) = delete;
	DictBuffer(DictBuffer &&other) : dict_buffer(other.dict_buffer), capacity(other.capacity), size(other.size) {
		other.dict_buffer = nullptr;
		other.size = 0;
		other.capacity = 0;
	}
	DictBuffer &operator=(DictBuffer &other) = delete;
	DictBuffer &operator=(DictBuffer &&other) {
		free(dict_buffer);
		dict_buffer = other.dict_buffer;
		other.dict_buffer = nullptr;
		capacity = other.capacity;
		size = other.size;
		return *this;
	}

public:
	operator bool() {
		return dict_buffer != nullptr;
	}
	void SetSize(idx_t size_p) {
		D_ASSERT(size_p <= capacity);
		size = size_p;
	}
	idx_t Size() const {
		return size;
	}
	idx_t Capacity() const {
		return capacity;
	}
	void *Buffer() const {
		return dict_buffer;
	}

private:
	void *dict_buffer;
	idx_t capacity = 0;
	idx_t size = 0;
};

} // namespace duckdb
