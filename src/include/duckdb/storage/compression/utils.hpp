#pragma once

#include <functional>
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/storage/storage_info.hpp"

namespace duckdb {

using analyze_sample_func_t = std::function<void(Vector &, idx_t)>;

struct AnalyzeSamplingState {
public:
	static constexpr uint32_t VECTORS_TO_SKIP = STANDARD_ROW_GROUPS_SIZE / STANDARD_VECTOR_SIZE / 8;
	static constexpr uint32_t MINIMUM_TUPLES = 32;

public:
	AnalyzeSamplingState(analyze_sample_func_t sampler = nullptr) : sampler(sampler) {
	}

public:
	void SetSampler(analyze_sample_func_t sampler);
	bool Sample(Vector &vector, idx_t count);

private:
	//! The function to call when a vector should be sampled
	analyze_sample_func_t sampler;
	//! The amount of vectors we have already sampled
	idx_t sampled_vectors = 0;
	//! The total amount of vectors we have seen
	idx_t total_vectors = 0;
};

} // namespace duckdb
