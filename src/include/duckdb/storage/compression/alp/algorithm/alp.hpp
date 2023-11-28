//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/algorithm/alp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/storage/compression/alp/alp_constants.hpp"
#include "duckdb/storage/compression/alp/alp_utils.hpp"

#include <cmath>

namespace duckdb {

namespace alp {

struct AlpCombination {
	int8_t exponent;
	int8_t factor;
	uint64_t n_appearances;
	uint64_t estimated_compression_size;

	AlpCombination(int8_t exponent, int8_t factor, uint64_t n_appearances, uint64_t estimated_compression_size)
	    : exponent(exponent), factor(factor), n_appearances(n_appearances),
	      estimated_compression_size(estimated_compression_size) {
	}
};

template <class T, bool EMPTY>
class AlpCompressionState {
public:
	AlpCompressionState() : vector_exponent(0), vector_factor(0), exceptions_count(0), bit_width(0) {
	}

	void Reset() {
		vector_exponent = 0;
		vector_factor = 0;
		exceptions_count = 0;
		bit_width = 0;
	}

	void ResetCombinations() {
		best_k_combinations.clear();
	}

public:
	uint8_t vector_exponent;
	uint8_t vector_factor;
	uint16_t exceptions_count;
	uint16_t bit_width;
	uint64_t bp_size;
	uint64_t frame_of_reference;
	int64_t encoded_integers[AlpConstants::ALP_VECTOR_SIZE];
	T exceptions[AlpConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_positions[AlpConstants::ALP_VECTOR_SIZE];
	vector<AlpCombination> best_k_combinations;
	uint8_t values_encoded[AlpConstants::ALP_VECTOR_SIZE * 8];
};

template <class T, bool EMPTY>
struct AlpCompression {
	using State = AlpCompressionState<T, EMPTY>;
	static constexpr uint8_t EXACT_TYPE_BITSIZE = sizeof(T) * 8;

	/*
	 * Conversion from a Floating-Point number to Int64 without rounding
	 */
	static int64_t NumberToInt64(T n) {
		n = n + AlpTypedConstants<T>::MAGIC_NUMBER - AlpTypedConstants<T>::MAGIC_NUMBER;
		//! Special values which cannot be casted to int64 without an undefined behaviour
		if (!Value::IsFinite(n) || Value::IsNan(n) || n > AlpConstants::ENCODING_UPPER_LIMIT ||
		    n < AlpConstants::ENCODING_LOWER_LIMIT) {
			return AlpConstants::ENCODING_UPPER_LIMIT;
		}
		return static_cast<int64_t>(n);
	}

	/*
	 * Encoding a single value with ALP
	 */
	static int64_t EncodeValue(T value, int8_t exp_idx, int8_t factor_idx) {
		T tmp_encoded_value =
		    value * AlpTypedConstants<T>::EXP_ARR[exp_idx] * AlpTypedConstants<T>::FRAC_ARR[factor_idx];
		int64_t encoded_value = NumberToInt64(tmp_encoded_value);
		return encoded_value;
	}

	/*
	 * Decoding a single value with ALP
	 */
	static T DecodeValue(int64_t encoded_value, int8_t exp_idx, int8_t factor_idx) {
		//! The cast to T is needed to prevent a signed integer overflow
		T decoded_value = static_cast<T>(encoded_value) * AlpConstants::FACT_ARR[factor_idx] *
		                  AlpTypedConstants<T>::FRAC_ARR[exp_idx];
		return decoded_value;
	}

	/*
	 * Return TRUE if c1 is a better combination than c2
	 * First criteria is number of times it appears as best combination
	 * Second criteria is the estimated compression size
	 * Third criteria is bigger exponent
	 * Fourth criteria is bigger factor
	 */
	static bool CompareALPCombinations(const AlpCombination &c1, const AlpCombination &c2) {
		return (c1.n_appearances > c2.n_appearances) ||
		       (c1.n_appearances == c2.n_appearances &&
		        (c1.estimated_compression_size < c2.estimated_compression_size)) ||
		       ((c1.n_appearances == c2.n_appearances &&
		         c1.estimated_compression_size == c2.estimated_compression_size) &&
		        (c2.exponent < c1.exponent)) ||
		       ((c1.n_appearances == c2.n_appearances &&
		         c1.estimated_compression_size == c2.estimated_compression_size && c2.exponent == c1.exponent) &&
		        (c2.factor < c1.factor));
	}

	/*
	 * Dry compress a vector (ideally a sample) to estimate ALP compression size given a exponent and factor
	 */
	template <bool PENALIZE_EXCEPTIONS>
	static uint64_t DryCompressToEstimateSize(const vector<T> &in, int8_t exp_idx, int8_t factor_idx) {
		idx_t n_values = in.size();
		idx_t exceptions_count = 0;
		idx_t non_exceptions_count = 0;
		uint32_t estimated_bits_per_value = 0;
		uint64_t estimated_compression_size = 0;
		int64_t max_encoded_value = NumericLimits<int64_t>::Minimum();
		int64_t min_encoded_value = NumericLimits<int64_t>::Maximum();

		for (const T &value : in) {
			int64_t encoded_value = EncodeValue(value, exp_idx, factor_idx);
			T decoded_value = DecodeValue(encoded_value, exp_idx, factor_idx);
			if (decoded_value == value) {
				non_exceptions_count++;
				max_encoded_value = MaxValue(encoded_value, max_encoded_value);
				min_encoded_value = MinValue(encoded_value, min_encoded_value);
				continue;
			}
			exceptions_count++;
		}

		// We penalize combinations which yields to almost all exceptions
		if (PENALIZE_EXCEPTIONS && non_exceptions_count < 2) {
			return NumericLimits<uint64_t>::Maximum();
		}

		// Evaluate factor/exponent compression size (we optimize for FOR)
		uint64_t delta = (static_cast<uint64_t>(max_encoded_value) - static_cast<uint64_t>(min_encoded_value));
		estimated_bits_per_value = std::ceil(std::log2(delta + 1));
		estimated_compression_size += n_values * estimated_bits_per_value;
		estimated_compression_size +=
		    exceptions_count * (EXACT_TYPE_BITSIZE + (AlpConstants::EXCEPTION_POSITION_SIZE * 8));
		return estimated_compression_size;
	}

	/*
	 * Find the best combinations of factor-exponent from each vector sampled from a rowgroup
	 * This function is called once per segment
	 * This operates over ALP first level samples
	 */
	static void FindTopKCombinations(const vector<vector<T>> &vectors_sampled, State &state) {
		state.ResetCombinations();
		// We use a 'pair' to hash it easily
		map<pair<int8_t, int8_t>, uint64_t> best_k_combinations_hash;

		// For each vector sampled
		for (auto &sampled_vector : vectors_sampled) {
			idx_t n_samples = sampled_vector.size();
			int8_t best_factor = AlpTypedConstants<T>::MAX_EXPONENT;
			int8_t best_exponent = AlpTypedConstants<T>::MAX_EXPONENT;

			//! We start our optimization with the worst possible total bits obtained from compression
			idx_t best_total_bits = (n_samples * (EXACT_TYPE_BITSIZE + AlpConstants::EXCEPTION_POSITION_SIZE * 8)) +
			                        (n_samples * EXACT_TYPE_BITSIZE);

			// N of appearances is irrelevant at this phase; we search for the best compression for the vector
			AlpCombination best_combination = {best_exponent, best_factor, 0, best_total_bits};
			//! We try all combinations in search for the one which minimize the compression size
			for (int8_t exp_idx = AlpTypedConstants<T>::MAX_EXPONENT; exp_idx >= 0; exp_idx--) {
				for (int8_t factor_idx = exp_idx; factor_idx >= 0; factor_idx--) {
					uint64_t estimated_compression_size =
					    DryCompressToEstimateSize<true>(sampled_vector, exp_idx, factor_idx);
					AlpCombination current_combination = {exp_idx, factor_idx, 0, estimated_compression_size};
					if (CompareALPCombinations(current_combination, best_combination)) {
						best_combination = current_combination;
					}
				}
			}
			pair<int8_t, int8_t> best_combination_pair = make_pair(best_combination.exponent, best_combination.factor);
			best_k_combinations_hash[best_combination_pair]++;
		}

		// Convert our hash pairs to a Combination vector to be able to sort
		// FIXME: A MaxHeap would be faster although this vector is always of constant size
		vector<AlpCombination> best_k_combinations;
		for (auto const &combination : best_k_combinations_hash) {
			best_k_combinations.emplace_back(
			    combination.first.first,  // Exponent
			    combination.first.second, // Factor
			    combination.second,       // N of times it appeared (hash value)
			    0 // Compression size is irrelevant at this phase since we compare combinations from different vectors
			);
		}
		sort(best_k_combinations.begin(), best_k_combinations.end(), CompareALPCombinations);

		// Save k' best combinations
		for (idx_t i = 0; i < MinValue(AlpConstants::MAX_COMBINATIONS, (uint8_t)best_k_combinations.size()); i++) {
			state.best_k_combinations.push_back(best_k_combinations[i]);
		}
	}

	/*
	 * Find the best combination of factor-exponent for a vector from within the best k combinations
	 * This is ALP second level sampling
	 */
	static void FindBestFactorAndExponent(const vector<T> &input_vector, idx_t n_values, State &state) {
		//! We sample equidistant values within a vector; to do this we skip a fixed number of values
		vector<T> vector_sample;
		uint32_t idx_increments = MaxValue(1, (int32_t)std::ceil((double)n_values / AlpConstants::SAMPLES_PER_VECTOR));
		for (idx_t i = 0; i < n_values; i += idx_increments) {
			vector_sample.push_back(input_vector[i]);
		}

		uint8_t best_exponent = 0;
		uint8_t best_factor = 0;
		uint64_t best_total_bits = NumericLimits<uint64_t>::Maximum();
		idx_t worse_total_bits_counter = 0;

		//! We try each K combination in search for the one which minimize the compression size in the vector
		for (auto &combination : state.best_k_combinations) {
			int32_t exp_idx = combination.exponent;
			int32_t factor_idx = combination.factor;
			uint64_t estimated_compression_size = DryCompressToEstimateSize<false>(vector_sample, exp_idx, factor_idx);

			// If current compression size is worse (higher) or equal than the current best combination
			if (estimated_compression_size >= best_total_bits) {
				worse_total_bits_counter += 1;
				// Early exit strategy
				if (worse_total_bits_counter == AlpConstants::SAMPLING_EARLY_EXIT_THRESHOLD) {
					break;
				}
				continue;
			}
			// Otherwise we replace the best and continue trying with the next combination
			best_total_bits = estimated_compression_size;
			best_factor = factor_idx;
			best_exponent = exp_idx;
			worse_total_bits_counter = 0;
		}
		state.vector_exponent = best_exponent;
		state.vector_factor = best_factor;
	}

	/*
	 * ALP Compress
	 */
	static void Compress(const vector<T> &input_vector, idx_t n_values, const vector<uint16_t> &vector_null_positions,
	                     idx_t nulls_count, State &state) {
		if (state.best_k_combinations.size() > 1) {
			FindBestFactorAndExponent(input_vector, n_values, state);
		} else {
			state.vector_exponent = state.best_k_combinations[0].exponent;
			state.vector_factor = state.best_k_combinations[0].factor;
		}

		// Encoding Floating-Point to Int64
		//! We encode all the values regardless of their correctness to recover the original floating-point
		//! We detect exceptions later using a predicated comparison
		vector<T> tmp_decoded_values(n_values, 0); // Tmp array to check wether the encoded values are exceptions
		for (idx_t i = 0; i < n_values; i++) {
			T value = input_vector[i];
			int64_t encoded_value = EncodeValue(value, state.vector_exponent, state.vector_factor);
			T decoded_value = DecodeValue(encoded_value, state.vector_exponent, state.vector_factor);
			state.encoded_integers[i] = encoded_value;
			tmp_decoded_values[i] = decoded_value;
		}

		// Detecting exceptions with predicated comparison
		uint16_t exceptions_idx = 0;
		vector<uint64_t> exceptions_positions(n_values, 0);
		for (idx_t i = 0; i < n_values; i++) {
			T decoded_value = tmp_decoded_values[i];
			T actual_value = input_vector[i];
			auto is_exception = (decoded_value != actual_value);
			exceptions_positions[exceptions_idx] = i;
			exceptions_idx += is_exception;
		}

		// Finding first non exception value
		int64_t a_non_exception_value = 0;
		for (idx_t i = 0; i < n_values; i++) {
			if (i != exceptions_positions[i]) {
				a_non_exception_value = state.encoded_integers[i];
				break;
			}
		}
		// Replacing that first non exception value on the vector exceptions
		for (idx_t i = 0; i < exceptions_idx; i++) {
			idx_t exception_pos = exceptions_positions[i];
			T actual_value = input_vector[exception_pos];
			state.encoded_integers[exception_pos] = a_non_exception_value;
			state.exceptions[i] = actual_value;
			state.exceptions_positions[i] = exception_pos;
		}
		state.exceptions_count = exceptions_idx;

		// Replacing nulls with that first non exception value
		for (idx_t i = 0; i < nulls_count; i++) {
			uint16_t null_value_pos = vector_null_positions[i];
			state.encoded_integers[null_value_pos] = a_non_exception_value;
		}

		// Analyze FFOR
		auto min_value = NumericLimits<int64_t>::Maximum();
		auto max_value = NumericLimits<int64_t>::Minimum();
		for (auto &encoded_value : state.encoded_integers) {
			max_value = MaxValue(max_value, encoded_value);
			min_value = MinValue(min_value, encoded_value);
		}
		uint64_t min_max_diff = (static_cast<uint64_t>(max_value) - static_cast<uint64_t>(min_value));

		auto *u_encoded_integers = reinterpret_cast<uint64_t *>(state.encoded_integers);
		auto const u_min_value = static_cast<uint64_t>(min_value);

		// Subtract FOR
		if (!EMPTY) { //! We only execute the FOR if we are writing the data
			for (idx_t i = 0; i < n_values; i++) {
				u_encoded_integers[i] -= u_min_value;
			}
		}

		auto bit_width = BitpackingPrimitives::MinimumBitWidth<uint64_t, false>(min_max_diff);
		auto bp_size = BitpackingPrimitives::GetRequiredSize(n_values, bit_width);
		if (!EMPTY && bit_width > 0) { //! We only execute the BP if we are writing the data
			BitpackingPrimitives::PackBuffer<uint64_t, false>(state.values_encoded, u_encoded_integers, n_values,
			                                                  bit_width);
		}
		state.bit_width = bit_width; // in bits
		state.bp_size = bp_size;     // in bytes
		state.frame_of_reference = min_value;
	}

	/*
	 * Overload without specifying nulls
	 */
	static void Compress(const vector<T> &input_vector, idx_t n_values, State &state) {
		vector<uint16_t> vector_null_positions;
		idx_t nulls_count = 0;
		Compress(input_vector, n_values, vector_null_positions, nulls_count, state);
	}
};

template <class T>
struct AlpDecompression {
	static void Decompress(uint8_t *for_encoded, T *output, idx_t count, uint8_t vector_factor, uint8_t vector_exponent,
	                       uint16_t exceptions_count, T *exceptions, uint16_t *exceptions_positions,
	                       uint64_t frame_of_reference, uint8_t bit_width) {
		uint64_t factor = AlpConstants::FACT_ARR[vector_factor];
		T exponent = AlpTypedConstants<T>::FRAC_ARR[vector_exponent];

		// Bit Unpacking
		uint8_t for_decoded[AlpConstants::ALP_VECTOR_SIZE * 8] = {0};
		if (bit_width > 0) {
			BitpackingPrimitives::UnPackBuffer<uint64_t>(for_decoded, for_encoded, count, bit_width);
		}
		uint64_t *encoded_integers = reinterpret_cast<uint64_t *>(data_ptr_cast(for_decoded));

		// unFOR
		for (idx_t i = 0; i < count; i++) {
			encoded_integers[i] += frame_of_reference;
		}

		// Decoding
		for (idx_t i = 0; i < count; i++) {
			auto encoded_integer = encoded_integers[i];
			output[i] = static_cast<T>(static_cast<int64_t>(encoded_integer)) * factor * exponent;
		}

		// Exceptions Patching
		for (idx_t i = 0; i < exceptions_count; i++) {
			output[exceptions_positions[i]] = static_cast<T>(exceptions[i]);
		}
	}
};

} // namespace alp

} // namespace duckdb
