//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/algorithm/alp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alprd/shared.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/bitpacking.hpp"

namespace duckdb {

namespace alp {

template <class T, bool EMPTY>
class AlpRDCompressionState {
	using EXACT_TYPE = typename FloatingToExact<T>::type;
public:
	AlpRDCompressionState() : right_bw(0), left_bw(0), exceptions_count(0) {

	}

	void Reset() {
		left_bp_size = 0;
		right_bp_size = 0;
		exceptions_count = 0;
	}

public:
	uint8_t right_bw;
	uint8_t left_bw;
	uint16_t exceptions_count;
	uint8_t right_parts_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	uint8_t left_parts_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	uint16_t left_parts_dict[AlpRDConstants::DICTIONARY_SIZE];
	uint16_t exceptions[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_positions[AlpRDConstants::ALP_VECTOR_SIZE];
	idx_t left_bp_size;
	idx_t right_bp_size;
	unordered_map<uint16_t, uint16_t> left_parts_dict_map;
};


template <class T, bool EMPTY>
struct AlpRDCompression {
	using State = AlpRDCompressionState<T, EMPTY>;
	using EXACT_TYPE = typename FloatingToExact<T>::type;
	static constexpr uint8_t EXACT_TYPE_BITSIZE = sizeof(EXACT_TYPE) * 8;

	/*
	 * Estimate the bits per value of ALPRD within a sample
	 */
	static double EstimateCompressionSize(uint8_t right_bw, uint8_t left_bw, uint16_t exceptions_count,
	                                    uint64_t sample_count){
		double exceptions_size =
		    exceptions_count * ((AlpRDConstants::EXCEPTION_POSITION_SIZE + AlpRDConstants::EXCEPTION_SIZE) * 8);
		double estimated_size = right_bw + left_bw + (exceptions_size / sample_count);
		return estimated_size;
	}

	static double BuildLeftPartsDictionary(vector<EXACT_TYPE> values, uint8_t right_bw, uint8_t left_bw,
	                                       bool persist_dict, State &state){
		unordered_map<EXACT_TYPE , int32_t> left_parts_hash;
		vector<pair<int32_t, uint64_t>> left_parts_sorted_repetitions;

		// Building a hash for all the left parts and how many times they appear
		for (idx_t i = 0; i < values.size(); i++) {
			auto left_tmp = values[i] >> right_bw;
			left_parts_hash[left_tmp]++;
		}

		// We build a vector from the hash to be able to sort it by repetition count
		left_parts_sorted_repetitions.reserve(left_parts_hash.size());
		for (auto& hash_pair : left_parts_hash) {
			left_parts_sorted_repetitions.emplace_back(hash_pair.second, hash_pair.first);
		}
		sort(left_parts_sorted_repetitions.begin(), left_parts_sorted_repetitions.end(),
		     [](const pair<uint16_t, uint64_t>& a, const pair<uint16_t, uint64_t>& b) { return a.first > b.first; });

		// Exceptions are left parts which do not fit in the fixed dictionary size
		uint32_t exceptions_count = 0;
		for (idx_t i = AlpRDConstants::DICTIONARY_SIZE; i < left_parts_sorted_repetitions.size(); i++) {
			exceptions_count += left_parts_sorted_repetitions[i].first;
		}

		if (persist_dict){
			idx_t dict_idx = 0;
			for (; dict_idx < MinValue<uint64_t>(AlpRDConstants::DICTIONARY_SIZE, left_parts_sorted_repetitions.size()); dict_idx++) {
				//! The dict keys are mapped to the left part themselves
				state.left_parts_dict[dict_idx] = left_parts_sorted_repetitions[dict_idx].second;
				state.left_parts_dict_map.insert({state.left_parts_dict[dict_idx], dict_idx});
			}
			//! Pararelly we store a map of the dictionary to quickly resolve exceptions during encoding
			for (idx_t i = dict_idx + 1; i < left_parts_sorted_repetitions.size(); i++) {
				state.left_parts_dict_map.insert({left_parts_sorted_repetitions[i].second, i});
			}
			state.left_bw = AlpRDConstants::DICTIONARY_BW; //! No matter what, dictionary is of constant size
			state.right_bw = right_bw;

			D_ASSERT(state.left_bw > 0 && state.left_bw <= AlpRDConstants::CUTTING_LIMIT && state.right_bw > 0);
		}

		double estimated_size = EstimateCompressionSize(right_bw, AlpRDConstants::DICTIONARY_BW, exceptions_count, values.size());
		return estimated_size;
	}

	static double FindBestDictionary(vector<EXACT_TYPE> values, State &state){
		uint8_t l_bw = AlpRDConstants::DICTIONARY_BW;
		uint8_t r_bw = EXACT_TYPE_BITSIZE;
		double best_dict_size = NumericLimits<int32_t>::Maximum();
		//! Finding the best position to CUT the values
		for (idx_t i = 1; i <= AlpRDConstants::CUTTING_LIMIT; i++){
			uint8_t candidate_l_bw = i;
			uint8_t candidate_r_bw = EXACT_TYPE_BITSIZE - i;
			double estimated_size = BuildLeftPartsDictionary(values, candidate_r_bw, candidate_l_bw, false, state);
			if (estimated_size <= best_dict_size){
				l_bw = candidate_l_bw;
				r_bw = candidate_r_bw;
				best_dict_size = estimated_size;
			}
		}
		double best_estimated_size = BuildLeftPartsDictionary(values, r_bw, l_bw, true, state);
		return best_estimated_size;
	}

	static void Compress(vector<EXACT_TYPE> in, idx_t n_values, State &state){

		uint64_t right_parts[AlpRDConstants::ALP_VECTOR_SIZE];
		uint16_t left_parts[AlpRDConstants::ALP_VECTOR_SIZE];

		// Cutting the floating point values
		for (idx_t i = 0; i < n_values; i++) {
			EXACT_TYPE tmp = in[i];
			right_parts[i]  = tmp & ((1ULL << state.right_bw) - 1);
			left_parts[i] = (tmp >> state.right_bw);
		}

		// Dictionary encoding for left parts
		for (idx_t i = 0; i < n_values; i++) {
			uint16_t dictionary_index;
			auto dictionary_key = left_parts[i];
			if (state.left_parts_dict_map.find(dictionary_key) == state.left_parts_dict_map.end()) {
				//! If not found on the dictionary we store the smallest non-key index as exception (the dict size)
				dictionary_index = AlpRDConstants::DICTIONARY_SIZE;
			} else {
				dictionary_index = state.left_parts_dict_map[dictionary_key];
			}
			left_parts[i] = dictionary_index;

			//! Left parts not found in the dictionary are stored as exceptions
			if (dictionary_index >= AlpRDConstants::DICTIONARY_SIZE) {
				state.exceptions[state.exceptions_count] = dictionary_key;
				state.exceptions_positions[state.exceptions_count] = i;
				state.exceptions_count++;
			}
		}

		auto right_bp_size = BitpackingPrimitives::GetRequiredSize(n_values, state.right_bw);
		auto left_bp_size = BitpackingPrimitives::GetRequiredSize(n_values, state.left_bw);

		if (!EMPTY){
			// Bitpacking Left and Right parts
			BitpackingPrimitives::PackBuffer<uint16_t, false>(
			    state.left_parts_encoded,
			    left_parts,
			    n_values,
			    state.left_bw);
			BitpackingPrimitives::PackBuffer<uint64_t, false>(
			    state.right_parts_encoded,
			    right_parts,
			    n_values,
			    state.right_bw);
		}

		state.left_bp_size = left_bp_size;
		state.right_bp_size = right_bp_size;
	}

};


template <class T>
struct AlpRDDecompression {
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	static void Decompress(uint8_t* left_encoded, uint8_t* right_encoded, uint16_t* left_parts_dict, EXACT_TYPE* output,
	                       idx_t values_count, uint16_t exceptions_count, uint16_t* exceptions,
	                       uint16_t* exceptions_positions, uint8_t right_bit_width) {

		uint8_t left_decoded[AlpRDConstants::ALP_VECTOR_SIZE * 8] = {0};
		uint8_t right_decoded[AlpRDConstants::ALP_VECTOR_SIZE * 8] = {0};
		uint8_t left_bit_width = AlpRDConstants::DICTIONARY_BW;

		// Bitunpacking left and right parts
		BitpackingPrimitives::UnPackBuffer<uint16_t>(left_decoded, left_encoded, values_count, left_bit_width);
		BitpackingPrimitives::UnPackBuffer<EXACT_TYPE>(right_decoded, right_encoded, values_count, right_bit_width);

		uint16_t *left_parts = reinterpret_cast<uint16_t*>(data_ptr_cast(left_decoded));
		EXACT_TYPE *right_parts = reinterpret_cast<EXACT_TYPE*>(data_ptr_cast(right_decoded));

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