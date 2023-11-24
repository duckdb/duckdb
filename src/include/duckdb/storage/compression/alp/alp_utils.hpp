//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_utils.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/alp/alp_constants.hpp"
#include "duckdb/storage/compression/patas/patas.hpp"

#include <cmath>

namespace duckdb {

namespace alp {

struct AlpSamplingParameters {
	uint32_t n_lookup_values;
	uint32_t n_sampled_increments;
	uint32_t n_sampled_values;

	AlpSamplingParameters(uint32_t n_lookup_values, uint32_t n_sampled_increments, uint32_t n_sampled_values):
	      n_lookup_values(n_lookup_values), n_sampled_increments(n_sampled_increments), n_sampled_values(n_sampled_values) {
	}
};

class AlpUtils {
public:
	AlpUtils() {
	}

public:

	static AlpSamplingParameters GetSamplingParameters(idx_t current_vector_n_values){

		uint32_t n_lookup_values = MinValue(current_vector_n_values, (idx_t)AlpConstants::ALP_VECTOR_SIZE);
		//! We sample equidistant values within a vector; to do this we jump a fixed number of values
		uint32_t n_sampled_increments =
		    MaxValue(1, (int32_t)std::ceil((double)n_lookup_values / AlpConstants::SAMPLES_PER_VECTOR));
		uint32_t n_sampled_values = std::ceil((double)n_lookup_values / n_sampled_increments);
		D_ASSERT(n_sampled_values < AlpConstants::ALP_VECTOR_SIZE);

		AlpSamplingParameters sampling_params = {n_lookup_values, n_sampled_increments, n_sampled_values};
		return sampling_params;
	}

	static bool MustSkipSamplingFromCurrentVector(idx_t vectors_count, idx_t vectors_sampled_count,
	                                              idx_t current_vector_n_values) {
		//! We sample equidistant vectors; to do this we skip a fixed values of vectors
		bool must_select_rowgroup_samples = (vectors_count % AlpConstants::RG_SAMPLES_DUCKDB_JUMP) == 0;

		printf("hmmmmmm2!!! %d -", vectors_count);
		//! If we are not in the correct jump, we do not take sample from this vector
		if (!must_select_rowgroup_samples) {
			return true;
		}

		printf("hmmmmmm!!!");

		//! We do not take samples of non-complete duckdb vectors (usually the last one)
		//! Except in the case of too little data
		if (current_vector_n_values < AlpConstants::SAMPLES_PER_VECTOR && vectors_sampled_count != 0) {
			return true;
		}
		return false;
	}

	template <class T>
	static void ReplaceNullsInVector(vector<T> &input_vector, const vector<uint16_t> &vector_null_positions,
	                                 idx_t values_count, idx_t nulls_count) {
		// Finding the first non-null value
		idx_t tmp_null_idx = 0;
		// T a_non_null_raw_value = 0;
		// EXACT_TYPE a_non_null_value = 0;
		T a_non_null_value = 0;
		for (idx_t i = 0; i < values_count; i++) {
			if (i != vector_null_positions[tmp_null_idx]) {
				a_non_null_value = input_vector[i];
				// a_non_null_raw_value = raw_input_vector[i];
				break;
			}
			tmp_null_idx += 1;
		}
		// Replacing that first non-null value on the vector
		for (idx_t i = 0; i < nulls_count; i++) {
			uint16_t null_value_pos = vector_null_positions[i];
			input_vector[null_value_pos] = a_non_null_value;
			// raw_input_vector[null_value_pos] = a_non_null_raw_value;
		}
	}
};

} // namespace alp

} // namespace duckdb
