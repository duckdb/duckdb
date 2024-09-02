//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alprd/algorithm/alprd.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/patas/patas.hpp"
#include "duckdb/storage/compression/alprd/alprd_constants.hpp"

#include "duckdb/common/bitpacking.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include <cmath>

namespace duckdb {

namespace alp {

struct AlpRDLeftPartInfo {
	uint32_t count;
	uint64_t hash;
	AlpRDLeftPartInfo(uint32_t count, uint64_t hash) : count(count), hash(hash) {
	}
};

template <class T, bool EMPTY>
class AlpRDCompressionState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	AlpRDCompressionState() : right_bit_width(0), left_bit_width(0), exceptions_count(0) {
	}

	void Reset() {
		left_bit_packed_size = 0;
		right_bit_packed_size = 0;
		exceptions_count = 0;
	}

public:
	uint8_t right_bit_width; // 'right' & 'left' refer to the respective parts of the floating numbers after splitting
	uint8_t left_bit_width;
	uint16_t exceptions_count;
	uint8_t right_parts_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	uint8_t left_parts_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	uint16_t left_parts_dict[AlpRDConstants::MAX_DICTIONARY_SIZE];
	uint16_t exceptions[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_positions[AlpRDConstants::ALP_VECTOR_SIZE];
	idx_t left_bit_packed_size;
	idx_t right_bit_packed_size;
	unordered_map<uint16_t, uint16_t> left_parts_dict_map;
	uint8_t actual_dictionary_size;
};

template <class T, bool EMPTY>
struct AlpRDCompression {
	using State = AlpRDCompressionState<T, EMPTY>;
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;
	static constexpr uint8_t EXACT_TYPE_BITSIZE = sizeof(EXACT_TYPE) * 8;

	/*
	 * Estimate the bits per value of ALPRD within a sample
	 */
	static double EstimateCompressionSize(uint8_t right_bit_width, uint8_t left_bit_width, uint16_t exceptions_count,
	                                      uint64_t sample_count) {
		double exceptions_size =
		    exceptions_count * ((AlpRDConstants::EXCEPTION_POSITION_SIZE + AlpRDConstants::EXCEPTION_SIZE) * 8);
		double estimated_size =
		    right_bit_width + left_bit_width + (exceptions_size / static_cast<double>(sample_count));
		return estimated_size;
	}

	template <bool PERSIST_DICT>
	static double BuildLeftPartsDictionary(const vector<EXACT_TYPE> &values, uint8_t right_bit_width, State &state) {
		unordered_map<EXACT_TYPE, int32_t> left_parts_hash;
		vector<AlpRDLeftPartInfo> left_parts_sorted_repetitions;

		// Building a hash for all the left parts and how many times they appear
		for (auto &value : values) {
			auto left_tmp = value >> right_bit_width;
			left_parts_hash[left_tmp]++;
		}

		// We build a vector from the hash to be able to sort it by repetition count
		left_parts_sorted_repetitions.reserve(left_parts_hash.size());
		for (auto &hash_pair : left_parts_hash) {
			left_parts_sorted_repetitions.emplace_back(hash_pair.second, hash_pair.first);
		}
		sort(left_parts_sorted_repetitions.begin(), left_parts_sorted_repetitions.end(),
		     [](const AlpRDLeftPartInfo &a, const AlpRDLeftPartInfo &b) { return a.count > b.count; });

		// Exceptions are left parts which do not fit in the fixed dictionary size
		uint32_t exceptions_count = 0;
		for (idx_t i = AlpRDConstants::MAX_DICTIONARY_SIZE; i < left_parts_sorted_repetitions.size(); i++) {
			exceptions_count += left_parts_sorted_repetitions[i].count;
		}

		// The left parts bit width after compression is determined by how many elements are in the dictionary
		uint64_t actual_dictionary_size =
		    MinValue<uint64_t>(AlpRDConstants::MAX_DICTIONARY_SIZE, left_parts_sorted_repetitions.size());
		uint8_t left_bit_width =
		    MaxValue<uint8_t>(1, ExactNumericCast<uint8_t>(std::ceil(std::log2(actual_dictionary_size))));

		if (PERSIST_DICT) {
			for (idx_t dict_idx = 0; dict_idx < actual_dictionary_size; dict_idx++) {
				//! The dict keys are mapped to the left part themselves
				state.left_parts_dict[dict_idx] =
				    UnsafeNumericCast<uint16_t>(left_parts_sorted_repetitions[dict_idx].hash);
				state.left_parts_dict_map.insert({state.left_parts_dict[dict_idx], dict_idx});
			}
			//! Pararelly we store a map of the dictionary to quickly resolve exceptions during encoding
			for (idx_t i = actual_dictionary_size + 1; i < left_parts_sorted_repetitions.size(); i++) {
				state.left_parts_dict_map.insert({left_parts_sorted_repetitions[i].hash, i});
			}
			state.left_bit_width = left_bit_width;
			state.right_bit_width = right_bit_width;
			state.actual_dictionary_size = UnsafeNumericCast<uint8_t>(actual_dictionary_size);

			D_ASSERT(state.left_bit_width > 0 && state.right_bit_width > 0 &&
			         state.left_bit_width <= AlpRDConstants::MAX_DICTIONARY_BIT_WIDTH &&
			         state.actual_dictionary_size <= AlpRDConstants::MAX_DICTIONARY_SIZE);
		}

		double estimated_size = EstimateCompressionSize(right_bit_width, left_bit_width,
		                                                UnsafeNumericCast<uint16_t>(exceptions_count), values.size());
		return estimated_size;
	}

	static double FindBestDictionary(const vector<EXACT_TYPE> &values, State &state) {
		uint8_t right_bit_width = 0;
		double best_dict_size = NumericLimits<int32_t>::Maximum();
		//! Finding the best position to CUT the values
		for (idx_t i = 1; i <= AlpRDConstants::CUTTING_LIMIT; i++) {
			uint8_t candidate_right_bit_width = UnsafeNumericCast<uint8_t>(EXACT_TYPE_BITSIZE - i);
			double estimated_size = BuildLeftPartsDictionary<false>(values, candidate_right_bit_width, state);
			if (estimated_size <= best_dict_size) {
				right_bit_width = candidate_right_bit_width;
				best_dict_size = estimated_size;
			}
			// TODO: We could implement an early exit mechanism similar to normal ALP
		}
		double estimated_size = BuildLeftPartsDictionary<true>(values, right_bit_width, state);
		return estimated_size;
	}

	static void Compress(const EXACT_TYPE *input_vector, idx_t n_values, State &state) {

		uint64_t right_parts[AlpRDConstants::ALP_VECTOR_SIZE];
		uint16_t left_parts[AlpRDConstants::ALP_VECTOR_SIZE];

		// Cutting the floating point values
		for (idx_t i = 0; i < n_values; i++) {
			EXACT_TYPE tmp = input_vector[i];
			right_parts[i] = tmp & ((1ULL << state.right_bit_width) - 1);
			left_parts[i] = UnsafeNumericCast<uint16_t>(tmp >> state.right_bit_width);
		}

		// Dictionary encoding for left parts
		for (idx_t i = 0; i < n_values; i++) {
			uint16_t dictionary_index;
			auto dictionary_key = left_parts[i];
			if (state.left_parts_dict_map.find(dictionary_key) == state.left_parts_dict_map.end()) {
				//! If not found on the dictionary we store the smallest non-key index as exception (the dict size)
				dictionary_index = state.actual_dictionary_size;
			} else {
				dictionary_index = state.left_parts_dict_map[dictionary_key];
			}
			left_parts[i] = dictionary_index;

			//! Left parts not found in the dictionary are stored as exceptions
			if (dictionary_index >= state.actual_dictionary_size) {
				state.exceptions[state.exceptions_count] = dictionary_key;
				state.exceptions_positions[state.exceptions_count] = UnsafeNumericCast<uint16_t>(i);
				state.exceptions_count++;
			}
		}

		auto right_bit_packed_size = BitpackingPrimitives::GetRequiredSize(n_values, state.right_bit_width);
		auto left_bit_packed_size = BitpackingPrimitives::GetRequiredSize(n_values, state.left_bit_width);

		if (!EMPTY) {
			// Bitpacking Left and Right parts
			BitpackingPrimitives::PackBuffer<uint16_t, false>(state.left_parts_encoded, left_parts, n_values,
			                                                  state.left_bit_width);
			BitpackingPrimitives::PackBuffer<uint64_t, false>(state.right_parts_encoded, right_parts, n_values,
			                                                  state.right_bit_width);
		}

		state.left_bit_packed_size = left_bit_packed_size;
		state.right_bit_packed_size = right_bit_packed_size;
	}
};

template <class T>
struct AlpRDDecompression {
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	static void Decompress(uint8_t *left_encoded, uint8_t *right_encoded, const uint16_t *left_parts_dict,
	                       EXACT_TYPE *output, idx_t values_count, uint16_t exceptions_count,
	                       const uint16_t *exceptions, const uint16_t *exceptions_positions, uint8_t left_bit_width,
	                       uint8_t right_bit_width) {

		uint8_t left_decoded[AlpRDConstants::ALP_VECTOR_SIZE * 8] = {0};
		uint8_t right_decoded[AlpRDConstants::ALP_VECTOR_SIZE * 8] = {0};

		// Bitunpacking left and right parts
		BitpackingPrimitives::UnPackBuffer<uint16_t>(left_decoded, left_encoded, values_count, left_bit_width);
		BitpackingPrimitives::UnPackBuffer<EXACT_TYPE>(right_decoded, right_encoded, values_count, right_bit_width);

		uint16_t *left_parts = reinterpret_cast<uint16_t *>(data_ptr_cast(left_decoded));
		EXACT_TYPE *right_parts = reinterpret_cast<EXACT_TYPE *>(data_ptr_cast(right_decoded));

		// Decoding
		for (idx_t i = 0; i < values_count; i++) {
			uint16_t left = left_parts_dict[left_parts[i]];
			EXACT_TYPE right = right_parts[i];
			output[i] = (static_cast<EXACT_TYPE>(left) << right_bit_width) | right;
		}

		// Exceptions Patching (exceptions only occur in left parts)
		for (idx_t i = 0; i < exceptions_count; i++) {
			EXACT_TYPE right = right_parts[exceptions_positions[i]];
			uint16_t left = exceptions[i];
			output[exceptions_positions[i]] = (static_cast<EXACT_TYPE>(left) << right_bit_width) | right;
		}
	}
};

} // namespace alp

} // namespace duckdb
