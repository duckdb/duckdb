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
	AlpRDCompressionState() : left_bw(0), right_bw(0), exceptions_count(0) {

	}

	void Reset() {
		left_bp_size = 0;
		right_bp_size = 0;
		exceptions_count = 0;
	}

	void ResetDictionary(){
		dict_map.clear();
	}

public:
	uint8_t right_parts_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	uint8_t left_parts_encoded[AlpRDConstants::ALP_VECTOR_SIZE * 8];
	uint16_t exceptions_count;
	uint16_t dict[AlpRDConstants::DICTIONARY_SIZE];
	idx_t dictionary_count;
	uint64_t right_parts[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t left_parts[AlpRDConstants::ALP_VECTOR_SIZE];
	uint8_t right_bw;
	uint8_t left_bw;
	EXACT_TYPE exceptions[AlpRDConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_positions[AlpRDConstants::ALP_VECTOR_SIZE];
	idx_t left_bp_size;
	idx_t right_bp_size;
	unordered_map<uint16_t, uint16_t> dict_map;


};


template <class T, bool EMPTY>
struct AlpRDCompression {
	using State = AlpRDCompressionState<T, EMPTY>;
	using EXACT_TYPE = typename FloatingToExact<T>::type;
	static constexpr uint8_t EXACT_TYPE_BITSIZE = sizeof(EXACT_TYPE) * 8;

	static double EstimateCompressionSize(uint8_t right_bw, uint8_t left_bw, uint16_t exceptions_count,
	                                    uint64_t sample_count){
		// TODO: Use 2 instead of 8 (on a variable called EXCEPTION_SIZE)
		double exceptions_size = exceptions_count * ((AlpRDConstants::EXCEPTION_POSITION_SIZE + AlpRDConstants::EXCEPTION_SIZE) * 8);
		double estimated_size = right_bw + left_bw + (exceptions_size / sample_count);
		return estimated_size;
	}

	static inline bool SortAsc(const std::pair<uint16_t, uint64_t>& a, const std::pair<uint16_t, uint64_t>& b) {
		return (a.first > b.first);
	}

	static double BuildDictionary(vector<EXACT_TYPE> in, uint8_t right_bw, uint8_t left_bw, bool store_dict, State &state){
		unordered_map<EXACT_TYPE , int> hash;
		vector<pair<int, uint64_t>> repetition_vec;

		for (idx_t j = 0; j < in.size(); j++) {
			auto tmp = in[j] >> right_bw;
			hash[tmp]++;
		}

		repetition_vec.reserve(hash.size());
		for (auto& pair : hash) {
			repetition_vec.emplace_back(pair.second, pair.first);
		}

		std::sort(repetition_vec.begin(), repetition_vec.end(), SortAsc);

		uint32_t exp_c = 0;
		for (idx_t i = AlpRDConstants::DICTIONARY_SIZE; i < repetition_vec.size(); ++i) {
			exp_c += repetition_vec[i].first;
		}
		printf("- Exception count: %d \n", exp_c);
		if (store_dict){
			idx_t c = 0;
			for (; c < MinValue<uint64_t>(AlpRDConstants::DICTIONARY_SIZE, repetition_vec.size()); c++) {
				state.dict[c] = repetition_vec[c].second; // the dict key
				printf("DICTIONARY KEY %d\n", c);
				printf("DICTIONARY VALUE %d\n", repetition_vec[c].second);
				state.dict_map.insert({state.dict[c], c});
			}
			for (size_t i {c + 1}; i < repetition_vec.size(); ++i) {
				state.dict_map.insert({repetition_vec[i].second, i});
			}
			state.dictionary_count = c; //! TODO: Why needed if c is always equal to DICTIONARY_SIZE?
			printf("Dictionary COUNT %d\n", c);
			state.left_bw = AlpRDConstants::DICTIONARY_BW; //! No matter what, dictionary is of constant size
			state.right_bw = right_bw;
		}

		double estimated_size = EstimateCompressionSize(right_bw, AlpRDConstants::DICTIONARY_BW, exp_c, in.size());
		return estimated_size;
	}

	static double FindBestDictionary(vector<EXACT_TYPE> smp_arr, State &state){
		uint8_t l_bw = AlpRDConstants::DICTIONARY_BW;
		uint8_t r_bw = EXACT_TYPE_BITSIZE;
		double best_size = std::numeric_limits<uint32_t>::max();
		for (idx_t i = 1; i <= AlpRDConstants::CUTTING_LIMIT; i++){
			uint8_t candidate_l_bw = i;
			uint8_t candidate_r_bw = EXACT_TYPE_BITSIZE - i;
			double estimated_size = BuildDictionary(smp_arr, candidate_r_bw, candidate_l_bw, false, state);
			printf("estimated size for %d bw is: %f \n", candidate_r_bw, estimated_size);
			if (estimated_size <= best_size){
				l_bw = candidate_l_bw;
				r_bw = candidate_r_bw;
				best_size = estimated_size;
			}
		}
		printf("Here %d \n", r_bw);
		double best_estimated_size = BuildDictionary(smp_arr, r_bw, l_bw, true, state);
		printf("maybe here %f \n", best_estimated_size);
		return best_estimated_size;
	}

	static void Compress(vector<EXACT_TYPE> in, idx_t n_values, State &state){

		// CUT
		//state.right_parts[0] = (int64_t)(128);
		for (idx_t i = 0; i < n_values; i++) {
			EXACT_TYPE tmp = in[i];
			// state.right_parts[i]  = static_cast<int64_t>(128); // first arr //TODO: This is wrong for float
			state.right_parts[i]  = tmp & ((1ULL << state.right_bw) - 1);
			state.left_parts[i] = (tmp >> state.right_bw); // second arr
//			uint64_t n = state.right_parts[i];
//			uint64_t j = 1UL<<(sizeof(n)*CHAR_BIT-1);
//			while(j>0){
//				if(n&j)
//					printf("1");
//				else
//					printf("0");
//				j >>= 1;
//			}
//			printf("\n");
		}

		// DICTIONARY COMPRESSION OF LEFT PARTS
		for (idx_t i = 0; i < n_values; i++) {
			uint16_t dictionary_index;
			auto dictionary_key = state.left_parts[i];
			if (state.dict_map.find(dictionary_key) == state.dict_map.end()) {
				//! Not found on the dictionary we store the smallest index for exception (the dictionary size)
				dictionary_index = AlpRDConstants::DICTIONARY_SIZE; //! TODO: Ask azim why here he used dict_count
			} else {
				dictionary_index = state.dict_map[dictionary_key]; // Key can exist but index maybe > size
			}
			state.left_parts[i] = dictionary_index;
//
			printf("C LEFT %d | ", dictionary_key);
			printf("C LEFT IDX %d | ", dictionary_index);
			printf("C RIGHT %lu | ", state.right_parts[i]);

			// in those cases we store them as exceptions
			if (dictionary_index >= AlpRDConstants::DICTIONARY_SIZE) {
				state.exceptions[state.exceptions_count] = in[i]; //! TODO: Here exception should be only left
				state.exceptions_positions[state.exceptions_count] = i;
				state.exceptions_count++;
			}
		}

		printf("Found %d exceptions\n", state.exceptions_count);
		printf("First exception compress %f\n", state.exceptions[0]);

		auto right_bp_size = BitpackingPrimitives::GetRequiredSize(n_values, state.right_bw);
		auto left_bp_size = BitpackingPrimitives::GetRequiredSize(n_values, state.left_bw);

		// Analyze FFOR
		auto max = std::numeric_limits<uint64_t>::min();
		for (idx_t i = 0; i < n_values; i++) {
			if (state.right_parts[i] > max) { max = state.right_parts[i]; }
		}
		auto r_minimum_bw = BitpackingPrimitives::MinimumBitWidth<uint64_t, false>(max);


		printf("IN SIZE %d\n", n_values);
		printf("LEFT BW %d\n", state.left_bw);
		printf("LEFT BP SIZE %d\n", left_bp_size);
		printf("RIGHT BW %d\n", state.right_bw);
		printf("RIGHT BP SIZE %d\n", right_bp_size);
		printf("MINIMUM BW KKKK %d\n", r_minimum_bw);

		D_ASSERT(state.left_bw > 0 && state.right_bw > 0);

		//printf(typeid(EXACT_TYPE).name());
//		printf("\n");
//		printf(__PRETTY_FUNCTION__);
//		printf("\n");
//		printf("%d\n\n", EXACT_TYPE_BITSIZE);

		//uint8_t right_parts_encoded_tmp[AlpRDConstants::ALP_VECTOR_SIZE * 8];

		if (!EMPTY){ // only if not analyze
			BitpackingPrimitives::PackBuffer<uint16_t, false>(
			    state.left_parts_encoded,
			    state.left_parts, // TODO: Left parts can be local instead of state variable
			    n_values,
			    state.left_bw);
			printf("Bitpacking left parts done\n");
			BitpackingPrimitives::PackBuffer<uint64_t, false>(
			    state.right_parts_encoded,
			    state.right_parts, // TODO: Right parts can be local instead of state variable
			    n_values,
			    state.right_bw);
		}
		printf("Bitpacking parts done\n");
		state.left_bp_size = left_bp_size; // in bytes
		state.right_bp_size = right_bp_size; // in bytes
	}

};


template <class T>
struct AlpRDDecompression {
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	static void Decompress(uint8_t *left_encoded, uint8_t *right_encoded, uint16_t* dict, EXACT_TYPE* out, idx_t count,
	                       uint16_t exceptions_count, EXACT_TYPE* exceptions, uint16_t* exceptions_positions,
	                       uint8_t right_bit_width) {
		printf("right_bit_width %d, exceptions_count %d", right_bit_width, exceptions_count);

		uint8_t left_decoded[AlpRDConstants::ALP_VECTOR_SIZE * 8] = {0};
		uint8_t right_decoded[AlpRDConstants::ALP_VECTOR_SIZE * 8] = {0};
		uint8_t left_bit_width = AlpRDConstants::DICTIONARY_BW;

		BitpackingPrimitives::UnPackBuffer<uint16_t>(left_decoded, left_encoded, count, left_bit_width);
		BitpackingPrimitives::UnPackBuffer<EXACT_TYPE>(right_decoded, right_encoded, count, right_bit_width);

		uint16_t *left_parts = reinterpret_cast<uint16_t*>(data_ptr_cast(left_decoded));
		EXACT_TYPE *right_parts = reinterpret_cast<EXACT_TYPE*>(data_ptr_cast(right_decoded));
		//uint16_t *dict = reinterpret_cast<uint16_t*>(data_ptr_cast(dict_encoded));

		// Decoding
		for (idx_t i = 0; i < count; i++) {
			uint16_t left = dict[left_parts[i]];
			printf("D LEFT %d | ", left);
//			printf("D LEFT IDX %d | ", left_parts[i]);
			EXACT_TYPE right = right_parts[i];
			printf("D RIGHT %lu | ", right_parts[i]);
			out[i] = (static_cast<EXACT_TYPE>(left) << right_bit_width) | right;
		}

		//! TODO: Here the patch should only occur on the left side; otherwise we are wasting space
		// Exceptions Patching
		// printf("First exception %f\n", static_cast<T>(exceptions[0]));
		for (idx_t i = 0; i < exceptions_count; i++) {
			out[exceptions_positions[i]] = exceptions[i];
		}
	}
};

} // namespace alp

} // namespace duckdb