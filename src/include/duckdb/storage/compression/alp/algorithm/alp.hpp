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
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/hash.hpp"
#include "duckdb/storage/compression/alp/alp_constants.hpp"
#include "duckdb/storage/compression/alp/alp_utils.hpp"

#include <cmath>

namespace duckdb {

namespace alp {

struct AlpEncodingIndices {
	uint8_t exponent;
	uint8_t factor;

	AlpEncodingIndices(uint8_t exponent, uint8_t factor) : exponent(exponent), factor(factor) {
	}

	AlpEncodingIndices() : exponent(0), factor(0) {
	}
};

struct AlpEncodingIndicesEquality {
	bool operator()(const AlpEncodingIndices &a, const AlpEncodingIndices &b) const {
		return a.exponent == b.exponent && a.factor == b.factor;
	}
};

struct AlpEncodingIndicesHash {
	hash_t operator()(const AlpEncodingIndices &encoding_indices) const {
		hash_t h1 = Hash<uint8_t>(encoding_indices.exponent);
		hash_t h2 = Hash<uint8_t>(encoding_indices.factor);
		return CombineHash(h1, h2);
	}
};

struct AlpCombination {
	AlpEncodingIndices encoding_indices;
	uint64_t n_appearances;
	uint64_t estimated_compression_size;

	AlpCombination(AlpEncodingIndices encoding_indices, uint64_t n_appearances, uint64_t estimated_compression_size)
	    : encoding_indices(encoding_indices), n_appearances(n_appearances),
	      estimated_compression_size(estimated_compression_size) {
	}
};

template <class T, bool EMPTY>
class AlpCompressionState {
public:
	AlpCompressionState() : vector_encoding_indices(0, 0), exceptions_count(0), bit_width(0) {
	}

	void Reset() {
		vector_encoding_indices = {0, 0};
		exceptions_count = 0;
		bit_width = 0;
	}

	void ResetCombinations() {
		best_k_combinations.clear();
	}

public:
	AlpEncodingIndices vector_encoding_indices;
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
	 * Check for special values which are impossible for ALP to encode
	 * because they cannot be cast to int64 without an undefined behaviour
	 */
	static bool IsImpossibleToEncode(T n) {
		return !Value::IsFinite(n) || Value::IsNan(n) || n > AlpConstants::ENCODING_UPPER_LIMIT ||
		       n < AlpConstants::ENCODING_LOWER_LIMIT || (n == 0.0 && std::signbit(n)); //! Verification for -0.0
	}

	/*
	 * Conversion from a Floating-Point number to Int64 without rounding
	 */
	static int64_t NumberToInt64(T n) {
		if (IsImpossibleToEncode(n)) {
			return NumericCast<int64_t>(AlpConstants::ENCODING_UPPER_LIMIT);
		}
		n = n + AlpTypedConstants<T>::MAGIC_NUMBER - AlpTypedConstants<T>::MAGIC_NUMBER;
		return NumericCast<int64_t>(n);
	}

	/*
	 * Encoding a single value with ALP
	 */
	static int64_t EncodeValue(T value, AlpEncodingIndices encoding_indices) {
		T tmp_encoded_value = value * AlpTypedConstants<T>::EXP_ARR[encoding_indices.exponent] *
		                      AlpTypedConstants<T>::FRAC_ARR[encoding_indices.factor];
		int64_t encoded_value = NumberToInt64(tmp_encoded_value);
		return encoded_value;
	}

	/*
	 * Decoding a single value with ALP
	 */
	static T DecodeValue(int64_t encoded_value, AlpEncodingIndices encoding_indices) {
		//! The cast to T is needed to prevent a signed integer overflow
		T decoded_value = static_cast<T>(encoded_value) * AlpConstants::FACT_ARR[encoding_indices.factor] *
		                  AlpTypedConstants<T>::FRAC_ARR[encoding_indices.exponent];
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
		        (c2.encoding_indices.exponent < c1.encoding_indices.exponent)) ||
		       ((c1.n_appearances == c2.n_appearances &&
		         c1.estimated_compression_size == c2.estimated_compression_size &&
		         c2.encoding_indices.exponent == c1.encoding_indices.exponent) &&
		        (c2.encoding_indices.factor < c1.encoding_indices.factor));
	}

	/*
	 * Dry compress a vector (ideally a sample) to estimate ALP compression size given a exponent and factor
	 */
	template <bool PENALIZE_EXCEPTIONS>
	static uint64_t DryCompressToEstimateSize(const vector<T> &input_vector, AlpEncodingIndices encoding_indices) {
		idx_t n_values = input_vector.size();
		idx_t exceptions_count = 0;
		idx_t non_exceptions_count = 0;
		uint32_t estimated_bits_per_value = 0;
		uint64_t estimated_compression_size = 0;
		int64_t max_encoded_value = NumericLimits<int64_t>::Minimum();
		int64_t min_encoded_value = NumericLimits<int64_t>::Maximum();

		for (const T &value : input_vector) {
			int64_t encoded_value = EncodeValue(value, encoding_indices);
			T decoded_value = DecodeValue(encoded_value, encoding_indices);
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
		estimated_bits_per_value = NumericCast<uint32_t>(std::ceil(std::log2(delta + 1)));
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

		unordered_map<AlpEncodingIndices, uint64_t, AlpEncodingIndicesHash, AlpEncodingIndicesEquality>
		    best_k_combinations_hash;
		// For each vector sampled
		for (auto &sampled_vector : vectors_sampled) {
			idx_t n_samples = sampled_vector.size();
			AlpEncodingIndices best_encoding_indices = {AlpTypedConstants<T>::MAX_EXPONENT,
			                                            AlpTypedConstants<T>::MAX_EXPONENT};

			//! We start our optimization with the worst possible total bits obtained from compression
			idx_t best_total_bits = (n_samples * (EXACT_TYPE_BITSIZE + AlpConstants::EXCEPTION_POSITION_SIZE * 8)) +
			                        (n_samples * EXACT_TYPE_BITSIZE);

			// N of appearances is irrelevant at this phase; we search for the best compression for the vector
			AlpCombination best_combination = {best_encoding_indices, 0, best_total_bits};
			//! We try all combinations in search for the one which minimize the compression size
			for (int8_t exp_idx = AlpTypedConstants<T>::MAX_EXPONENT; exp_idx >= 0; exp_idx--) {
				for (int8_t factor_idx = exp_idx; factor_idx >= 0; factor_idx--) {
					AlpEncodingIndices current_encoding_indices = {(uint8_t)exp_idx, (uint8_t)factor_idx};
					uint64_t estimated_compression_size =
					    DryCompressToEstimateSize<true>(sampled_vector, current_encoding_indices);
					AlpCombination current_combination = {current_encoding_indices, 0, estimated_compression_size};
					if (CompareALPCombinations(current_combination, best_combination)) {
						best_combination = current_combination;
					}
				}
			}
			best_k_combinations_hash[best_combination.encoding_indices]++;
		}

		// Convert our hash to a Combination vector to be able to sort
		// Note that this vector is always small (< 10 combinations)
		vector<AlpCombination> best_k_combinations;
		for (auto const &combination : best_k_combinations_hash) {
			best_k_combinations.emplace_back(
			    combination.first,  // Encoding Indices
			    combination.second, // N of times it appeared (hash value)
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
	static void FindBestFactorAndExponent(const T *input_vector, idx_t n_values, State &state) {
		//! We sample equidistant values within a vector; to do this we skip a fixed number of values
		vector<T> vector_sample;
		auto idx_increments = MaxValue<uint32_t>(
		    1, UnsafeNumericCast<uint32_t>(std::ceil((double)n_values / AlpConstants::SAMPLES_PER_VECTOR)));
		for (idx_t i = 0; i < n_values; i += idx_increments) {
			vector_sample.push_back(input_vector[i]);
		}

		AlpEncodingIndices best_encoding_indices = {0, 0};
		uint64_t best_total_bits = NumericLimits<uint64_t>::Maximum();
		idx_t worse_total_bits_counter = 0;

		//! We try each K combination in search for the one which minimize the compression size in the vector
		for (auto &combination : state.best_k_combinations) {
			uint64_t estimated_compression_size =
			    DryCompressToEstimateSize<false>(vector_sample, combination.encoding_indices);

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
			best_encoding_indices = combination.encoding_indices;
			worse_total_bits_counter = 0;
		}
		state.vector_encoding_indices = best_encoding_indices;
	}

	/*
	 * ALP Compress
	 */
	static void Compress(const T *input_vector, idx_t n_values, const uint16_t *vector_null_positions,
	                     idx_t nulls_count, State &state) {
		if (state.best_k_combinations.size() > 1) {
			FindBestFactorAndExponent(input_vector, n_values, state);
		} else {
			state.vector_encoding_indices = state.best_k_combinations[0].encoding_indices;
		}

		// Encoding Floating-Point to Int64
		//! We encode all the values regardless of their correctness to recover the original floating-point
		uint16_t exceptions_idx = 0;
		for (idx_t i = 0; i < n_values; i++) {
			T actual_value = input_vector[i];
			int64_t encoded_value = EncodeValue(actual_value, state.vector_encoding_indices);
			T decoded_value = DecodeValue(encoded_value, state.vector_encoding_indices);
			state.encoded_integers[i] = encoded_value;
			//! We detect exceptions using a predicated comparison
			auto is_exception = (decoded_value != actual_value);
			state.exceptions_positions[exceptions_idx] = UnsafeNumericCast<uint16_t>(i);
			exceptions_idx += is_exception;
		}

		// Finding first non exception value
		int64_t a_non_exception_value = 0;
		for (idx_t i = 0; i < n_values; i++) {
			if (i != state.exceptions_positions[i]) {
				a_non_exception_value = state.encoded_integers[i];
				break;
			}
		}
		// Replacing that first non exception value on the vector exceptions
		for (idx_t i = 0; i < exceptions_idx; i++) {
			idx_t exception_pos = state.exceptions_positions[i];
			T actual_value = input_vector[exception_pos];
			state.encoded_integers[exception_pos] = a_non_exception_value;
			state.exceptions[i] = actual_value;
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
		for (idx_t i = 0; i < n_values; i++) {
			max_value = MaxValue(max_value, state.encoded_integers[i]);
			min_value = MinValue(min_value, state.encoded_integers[i]);
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
		state.bit_width = bit_width;                                 // in bits
		state.bp_size = bp_size;                                     // in bytes
		state.frame_of_reference = static_cast<uint64_t>(min_value); // understood this can be negative
	}

	/*
	 * Overload without specifying nulls
	 */
	static void Compress(const T *input_vector, idx_t n_values, State &state) {
		Compress(input_vector, n_values, nullptr, 0, state);
	}
};

template <class T>
struct AlpDecompression {
	static void Decompress(uint8_t *for_encoded, T *output, idx_t count, uint8_t vector_factor, uint8_t vector_exponent,
	                       uint16_t exceptions_count, T *exceptions, const uint16_t *exceptions_positions,
	                       uint64_t frame_of_reference, uint8_t bit_width) {
		AlpEncodingIndices encoding_indices = {vector_exponent, vector_factor};

		// Bit Unpacking
		uint8_t for_decoded[AlpConstants::ALP_VECTOR_SIZE * 8] = {0};
		if (bit_width > 0) {
			BitpackingPrimitives::UnPackBuffer<uint64_t>(for_decoded, for_encoded, count, bit_width);
		}
		auto *encoded_integers = reinterpret_cast<uint64_t *>(data_ptr_cast(for_decoded));

		// unFOR
		for (idx_t i = 0; i < count; i++) {
			encoded_integers[i] += frame_of_reference;
		}

		// Decoding
		for (idx_t i = 0; i < count; i++) {
			auto encoded_integer = static_cast<int64_t>(encoded_integers[i]);
			output[i] = alp::AlpCompression<T, true>::DecodeValue(encoded_integer, encoding_indices);
		}

		// Exceptions Patching
		for (idx_t i = 0; i < exceptions_count; i++) {
			output[exceptions_positions[i]] = static_cast<T>(exceptions[i]);
		}
	}
};

} // namespace alp

} // namespace duckdb
