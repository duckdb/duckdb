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
#include "duckdb/storage/compression/utils.hpp"

namespace duckdb {

class ZSTDSamplingState {
public:
	ZSTDSamplingState();

public:
	void Sample(Vector &vec, idx_t count) {
		sampling_state.Sample(vec, count);
	}
	void Reset();

public:
	AnalyzeSamplingState sampling_state;

	idx_t total_sample_size = 0;
	AllocatedData sample_buffer;
	vector<idx_t> sample_sizes;
};

class DictBuffer {
public:
	DictBuffer() : dict_buffer(nullptr), capacity(0), size(0) {
	}
	DictBuffer(uint32_t capacity) : dict_buffer(nullptr), capacity(capacity), size(capacity) {
		owned_buffer = malloc(capacity);
		dict_buffer = owned_buffer;
	}
	DictBuffer(void *buffer, uint32_t size) : dict_buffer(buffer), capacity(size), size(size) {
		D_ASSERT(dict_buffer);
	}
	~DictBuffer() {
		free(owned_buffer);
	}
	DictBuffer(const DictBuffer &other) = delete;
	DictBuffer(DictBuffer &&other)
	    : owned_buffer(other.owned_buffer), dict_buffer(other.dict_buffer), capacity(other.capacity), size(other.size) {
		other.owned_buffer = nullptr;
		other.dict_buffer = nullptr;
		other.size = 0;
		other.capacity = 0;
	}
	DictBuffer &operator=(DictBuffer &other) = delete;
	DictBuffer &operator=(DictBuffer &&other) {
		free(owned_buffer);
		dict_buffer = other.dict_buffer;
		owned_buffer = other.owned_buffer;
		other.dict_buffer = nullptr;
		other.owned_buffer = nullptr;
		capacity = other.capacity;
		size = other.size;
		return *this;
	}

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
	void *owned_buffer = nullptr;
	void *dict_buffer = nullptr;
	uint32_t capacity = 0;
	uint32_t size = 0;
};

} // namespace duckdb
