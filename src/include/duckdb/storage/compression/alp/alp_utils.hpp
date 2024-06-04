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

	AlpSamplingParameters(uint32_t n_lookup_values, uint32_t n_sampled_increments, uint32_t n_sampled_values)
	    : n_lookup_values(n_lookup_values), n_sampled_increments(n_sampled_increments),
	      n_sampled_values(n_sampled_values) {
	}
};

class AlpUtils {
public:
	AlpUtils() {
	}

public:
	static AlpSamplingParameters GetSamplingParameters(idx_t current_vector_n_values) {

		auto n_lookup_values =
		    NumericCast<uint32_t>(MinValue(current_vector_n_values, (idx_t)AlpConstants::ALP_VECTOR_SIZE));
		//! We sample equidistant values within a vector; to do this we jump a fixed number of values
		uint32_t n_sampled_increments = MaxValue<uint32_t>(
		    1, UnsafeNumericCast<uint32_t>(std::ceil((double)n_lookup_values / AlpConstants::SAMPLES_PER_VECTOR)));
		uint32_t n_sampled_values = NumericCast<uint32_t>(std::ceil((double)n_lookup_values / n_sampled_increments));
		D_ASSERT(n_sampled_values < AlpConstants::ALP_VECTOR_SIZE);

		AlpSamplingParameters sampling_params = {n_lookup_values, n_sampled_increments, n_sampled_values};
		return sampling_params;
	}

	static bool MustSkipSamplingFromCurrentVector(idx_t vectors_count, idx_t vectors_sampled_count,
	                                              idx_t current_vector_n_values) {
		//! We sample equidistant vectors; to do this we skip a fixed values of vectors
		bool must_select_rowgroup_samples = (vectors_count % AlpConstants::RG_SAMPLES_DUCKDB_JUMP) == 0;

		//! If we are not in the correct jump, we do not take sample from this vector
		if (!must_select_rowgroup_samples) {
			return true;
		}

		//! We do not take samples of non-complete duckdb vectors (usually the last one)
		//! Except in the case of too little data
		if (current_vector_n_values < AlpConstants::SAMPLES_PER_VECTOR && vectors_sampled_count != 0) {
			return true;
		}
		return false;
	}

	template <class T>
	static T FindFirstValueNotInPositionsArray(const T *input_vector, const uint16_t *positions, idx_t values_count) {
		T a_non_special_value = 0;
		for (idx_t i = 0; i < values_count; i++) {
			if (i != positions[i]) {
				a_non_special_value = input_vector[i];
				break;
			}
		}
		return a_non_special_value;
	}

	template <class T>
	static void ReplaceValueInVectorPositions(T *input_vector, const uint16_t *positions_to_replace,
	                                          idx_t special_values_count, T value_to_replace) {
		for (idx_t i = 0; i < special_values_count; i++) {
			uint16_t null_value_pos = positions_to_replace[i];
			input_vector[null_value_pos] = value_to_replace;
		}
	}

	template <class T>
	static void FindAndReplaceNullsInVector(T *input_vector, const uint16_t *vector_null_positions, idx_t values_count,
	                                        idx_t nulls_count) {
		if (nulls_count == 0) {
			return;
		}
		T a_non_null_value = FindFirstValueNotInPositionsArray(input_vector, vector_null_positions, values_count);
		ReplaceValueInVectorPositions(input_vector, vector_null_positions, nulls_count, a_non_null_value);
	}
};

} // namespace alp

} // namespace duckdb
