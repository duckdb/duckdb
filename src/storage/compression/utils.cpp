#include "duckdb/storage/compression/utils.hpp"

namespace duckdb {

void AnalyzeSamplingState::SetSampler(analyze_sample_func_t func) {
	sampler = std::move(func);
}

bool AnalyzeSamplingState::Sample(Vector &vec, idx_t count) {
	bool should_sample = (total_vectors % VECTORS_TO_SKIP) == 0;

	if (should_sample) {
		// We only sample vectors that are big enough, unless we haven't sampled anything yet
		should_sample = (count >= MINIMUM_TUPLES || sampled_vectors == 0);
	}

	if (should_sample) {
		sampled_vectors++;
		if (sampler) {
			sampler(vec, count);
		}
	}

	total_vectors++;
	return should_sample;
}

} // namespace duckdb
