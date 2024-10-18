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
	ZSTDSamplingState() : concatenated_samples(nullptr) {
		auto &to_sample_vectors = this->to_sample_vectors;
		auto &vector_sizes = this->vector_sizes;
		auto &total_sample_size = this->total_sample_size;

		sampling_state.SetSampler([&to_sample_vectors, &vector_sizes, &total_sample_size](Vector &vec, idx_t count) {
			UnifiedVectorFormat vdata;
			vec.ToUnifiedFormat(count, vdata);

			auto data = UnifiedVectorFormat::GetData<string_t>(vdata);
			for (idx_t i = 0; i < count; i++) {
				auto idx = vdata.sel->get_index(i);
				if (!vdata.validity.RowIsValid(idx)) {
					continue;
				}
				auto &str = data[idx];
				auto string_size = str.GetSize();
				total_sample_size += string_size;
			}

			to_sample_vectors.emplace_back(std::move(vec));
			vector_sizes.push_back(count);
		});
	}
	~ZSTDSamplingState() {
		free(concatenated_samples);
	}

public:
	void Sample(Vector &vec, idx_t count) {
		sampling_state.Sample(vec, count);
	}
	bool Finalize();
	void Reset();

public:
	bool finalized = false;
	vector<Vector> to_sample_vectors;
	vector<idx_t> vector_sizes;
	idx_t total_sample_size = 0;
	AnalyzeSamplingState sampling_state;
	//! Populated by Finalize
	void *concatenated_samples;
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
	void *owned_buffer = nullptr;
	void *dict_buffer = nullptr;
	uint32_t capacity = 0;
	uint32_t size = 0;
};

} // namespace duckdb
