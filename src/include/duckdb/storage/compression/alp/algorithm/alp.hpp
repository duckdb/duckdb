//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/algorithm/alp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alp/shared.hpp"

#include "duckdb/common/common.hpp"
#include "duckdb/common/bitpacking.hpp"

namespace duckdb {

namespace alp {

template <class T, bool EMPTY>
class AlpCompressionState {
public:
	AlpCompressionState() : v_exponent(0), v_factor(0), exceptions_count(0), bit_width(0) {

	}

	void Reset() {
		v_exponent = 0;
		v_factor = 0;
		exceptions_count = 0;
		bit_width = 0;
	}

	void ResetCombinations(){
		combinations.clear();
	}

public:
	uint8_t v_exponent;
	uint8_t v_factor;
	uint16_t exceptions_count;
	uint16_t bit_width;
	uint64_t bp_size;
	uint64_t frame_of_reference;
	int64_t dig[AlpConstants::ALP_VECTOR_SIZE];
	T exceptions[AlpConstants::ALP_VECTOR_SIZE];
	uint16_t exceptions_positions[AlpConstants::ALP_VECTOR_SIZE];
	vector<pair<uint8_t, uint8_t>> combinations;
	uint8_t encoded[AlpConstants::ALP_VECTOR_SIZE * 8];

};


template <class T, bool EMPTY>
struct AlpCompression {
	using State = AlpCompressionState<T, EMPTY>;
	static constexpr uint8_t EXACT_TYPE_BITSIZE = sizeof(T) * 8;

	static int64_t NumberToInt64(T n){
		n = n + AlpPrimitives<T>::MAGIC_NUMBER;
		return static_cast<int64_t>(n) - static_cast<int64_t>(AlpPrimitives<T>::MAGIC_NUMBER);
	}

	static int64_t NumberToInt64_WO(T n){
		n = n + AlpPrimitives<T>::MAGIC_NUMBER - AlpPrimitives<T>::MAGIC_NUMBER;
		//printf("N %f | ", n);
		if (std::isinf(n) || std::isnan(n) || n > std::numeric_limits<int64_t>::max()){ //! TODO: Better strategy to prevent overflow undefined behaviour
			return std::numeric_limits<int64_t>::max();
		}
//		if (n > std::numeric_limits<int64_t>::max()){ //! TODO: Better strategy to prevent overflow undefined behaviour
//			return std::numeric_limits<int64_t>::max();
//		}
		return static_cast<int64_t>(n);
	}

	static T NumberToInt64_T(T n){
		return n + AlpPrimitives<T>::MAGIC_NUMBER - AlpPrimitives<T>::MAGIC_NUMBER;
		//return static_cast<int64_t>(n) - static_cast<int64_t>(AlpPrimitives<T>::MAGIC_NUMBER);
	}

	/*
	 * Function to sort best combinations from each vector sampled from the rowgroup
	 * First criteria is number of times it appears
	 * Second criteria is bigger exponent
	 * Thrid criteria is bigger factor
	 */
	static bool CompareBestCombinations(
	    const std::pair<std::pair<int, int>, int>& t1,
	    const std::pair<std::pair<int, int>, int>& t2){
		return
		    (t1.second > t2.second) ||
		    (t1.second == t2.second && (t2.first.first < t1.first.first)) ||
		    ((t1.second == t2.second && t2.first.first == t1.first.first) && (t2.first.second < t1.first.second));
	}

	static void FindTopNCombinations(vector<vector<T>> vectors_sampled, State &state){

		map<pair<int, int>, int> global_combinations;

		// For each vector in the rg_sample
		for (auto &smp_arr : vectors_sampled){
			idx_t smp_per_vector = smp_arr.size();
			vector<pair<int, int>> local_combinations;
			uint8_t found_factor {AlpPrimitives<T>::MAX_EXPONENT};
			uint8_t found_exponent {AlpPrimitives<T>::MAX_EXPONENT};
			idx_t best_total_bits = (smp_per_vector * (EXACT_TYPE_BITSIZE + 16)) + (smp_per_vector * EXACT_TYPE_BITSIZE);

			// We test all combinations (~170 combinations)
			for (int exp_ref = AlpPrimitives<T>::MAX_EXPONENT; exp_ref >= 0; exp_ref--){
				for (int factor_idx = exp_ref; factor_idx >= 0; factor_idx--){
					printf("Factor %d | ", factor_idx);
					uint32_t exception_c {0};
					uint32_t matches_c {0};
					uint32_t bits_per_digit {0};
					uint64_t local_total_bits {0};
					int64_t  local_max_digits {std::numeric_limits<int64_t>().min()};
					int64_t  local_min_digits {std::numeric_limits<int64_t>().max()};
					for (idx_t i = 0; i < smp_per_vector; i++){
						int64_t digits;
						T  dbl = smp_arr[i]; // TODO: Change to EXACT TYPE
						//printf("dbl %d - ", +smp_arr[i]);
						T  orig;
						T  cd;

						cd     = dbl * AlpPrimitives<T>::EXP_ARR[exp_ref] * AlpPrimitives<T>::FRAC_ARR[factor_idx];
						digits = NumberToInt64_WO(cd);
						//! The cast to T is needed to prevent a signed integer overflow
						orig   = static_cast<T>(digits) * AlpConstants::FACT_ARR[factor_idx] * AlpPrimitives<T>::FRAC_ARR[exp_ref];
						if (orig == dbl) {
							matches_c++;
							if (digits > local_max_digits) { local_max_digits = digits; };
							if (digits < local_min_digits) { local_min_digits = digits; };
							continue;
						}
						exception_c++;
					}
					// We skip combinations which yields to all exceptions
					if (matches_c < 2) { // We skip combinations which yields to less than 2 matches
						continue;
					}
					printf("Factor2 %d | ", factor_idx);
					// Evaluate factor/exponent performance (we optimize for FOR)
					uint64_t delta = local_max_digits - local_min_digits;
					bits_per_digit = ceil(log2(delta + 1));
					local_total_bits += smp_per_vector * bits_per_digit;
					local_total_bits += exception_c * (EXACT_TYPE_BITSIZE + 16); // TODO: Depends on EXACT TYPE
					printf("Factor3 %d | ", factor_idx);
					if (
					    (local_total_bits < best_total_bits) ||
					    // We prefer bigger exponents
					    (local_total_bits == best_total_bits && (found_exponent < exp_ref)) ||
					    // We prefer bigger factors
					    ((local_total_bits == best_total_bits && found_exponent == exp_ref)
					     && (found_factor < factor_idx))
					){
						best_total_bits = local_total_bits;
						found_exponent = exp_ref;
						found_factor = factor_idx;
					}
				}
			}
			std::pair<int, int> cmb = std::make_pair(found_exponent, found_factor);
			printf("ff %d | ee %d \n", found_exponent, found_factor);
			if (global_combinations.count(cmb)){
				global_combinations[cmb] += 1;
			} else {
				global_combinations[cmb] = 1;
			}
		}
		std::vector<std::pair<std::pair<int, int>, int>> comb_pairs;
		// Convert map pairs to vector for sort
		for (auto const& itr : global_combinations){
			printf("fff %d | eee %d \n", itr.first.first, itr.first.second);
			comb_pairs.emplace_back(
			    itr.first, // Pair exp, fac
			    itr.second // N of times it appeared
			    );
		}
		// We sort combinations based on times they appeared
		std::sort(comb_pairs.begin(), comb_pairs.end(), CompareBestCombinations);
		// TODO: change to MinValue() instead of if
		uint32_t n_comb = AlpConstants::MAX_COMBINATIONS;
		if (comb_pairs.size() < AlpConstants::MAX_COMBINATIONS){
			n_comb = comb_pairs.size();
		}
		// Save best exp,fac pairs
		for (idx_t i {0}; i < n_comb; i++){
			printf("FACTOR %d | EXPONENT %d \n", comb_pairs[i].first.first, comb_pairs[i].first.second);
			state.combinations.push_back(comb_pairs[i].first);
		}
	}

	static void FindFactorAndExponent(vector<T> input_vector, idx_t n_values, State &state){

		// TODO: Move sampling to another function
		vector<T> smp_arr;
		uint32_t idx_increments = MinValue(1, (int) floor(n_values / AlpConstants::SAMPLES_PER_VECTOR));
		for (idx_t i = 0; i < n_values; i += idx_increments){
			smp_arr.push_back(input_vector[i]);
		}
		printf("My vector sample has %d elements \n", smp_arr.size());
		uint8_t  found_exponent {0};
		uint8_t  found_factor 	{0};
		uint64_t previous_bit_count {0};
		uint8_t  worse_threshold {0};
		idx_t top_n = state.combinations.size();
		uint32_t smp_size = smp_arr.size(); // AlpPrimitives::SAMPLES_PER_VECTOR;
		for (idx_t k {0}; k < top_n; k++){
			int exp_ref		 	= state.combinations[k].first;
			int factor_idx 		= state.combinations[k].second;
			uint32_t exception_c 		{0};
			uint32_t matches_c 			{0};
			uint32_t bits_per_digit 	{0};
			uint64_t local_total_bits 	{0};
			int64_t  local_max_digits	{std::numeric_limits<int64_t>().min()};
			int64_t  local_min_digits 	{std::numeric_limits<int64_t>().max()};

			// Test combination of exponent and factor for the sample
			for (idx_t i = 0; i < smp_size; ++i) {
				int64_t digits;
				T  dbl = smp_arr[i];
				T  orig;
				T  cd;

				cd     = dbl * AlpPrimitives<T>::EXP_ARR[exp_ref] * AlpPrimitives<T>::FRAC_ARR[factor_idx];
				digits = NumberToInt64_WO(cd);
				//! The cast to T is needed to prevent a signed integer overflow
				orig   = static_cast<T>(digits) * AlpConstants::FACT_ARR[factor_idx] * AlpPrimitives<T>::FRAC_ARR[exp_ref];
				if (orig == dbl) {
					matches_c++;
					if (digits > local_max_digits) { local_max_digits = digits; }
					if (digits < local_min_digits) { local_min_digits = digits; }
				} else {
					exception_c++;
				}
			}

			// Evaluate factor/exponent performance (we optimize for FOR)
			uint64_t delta          = local_max_digits - local_min_digits;
			bits_per_digit 			= ceil(log2(delta + 1));
			local_total_bits 		+= smp_size * bits_per_digit;
			local_total_bits 		+= exception_c * (EXACT_TYPE_BITSIZE + 16);

			if (k == 0) {  // First try with first combination
				previous_bit_count = local_total_bits;
				found_factor      = factor_idx;
				found_exponent    = exp_ref;
				continue; // Go to second
			}
			if (local_total_bits >= previous_bit_count){ // If current is worse or equal than previous
				worse_threshold += 1;
				if (worse_threshold == 2) { break; } // We stop only if two are worse
				continue;
			}
			// Otherwise we replace best and continue with next
			previous_bit_count = local_total_bits;
			found_factor      = factor_idx;
			found_exponent    = exp_ref;
			worse_threshold = 0;
		}
		state.v_exponent = found_exponent;
		state.v_factor   = found_factor;
	}

	static void Compress(vector<T> input_vector, idx_t n_values, State &state){
		int64_t  tmp_digit {0};
		uint16_t exc_c {0};
		T   cd {0};
		T   orig {0};
		uint64_t pos {0};
		vector<T> tmp_dbl_arr(n_values, 0);
		vector<uint64_t> tmp_index(n_values, 0);

		if (state.combinations.size() > 1){
			// TODO: Missing to take sample
			FindFactorAndExponent(input_vector, n_values, state);
		} else {
			state.v_exponent = state.combinations[0].first;
			state.v_factor = state.combinations[0].second;
		}

		printf("Encoding %d values\n",  n_values);
		printf("Exponent %d\n",  state.v_exponent);
		printf("Factor %d\n",  state.v_factor);

		for (idx_t i {0}; i < n_values; i++) {
			auto dbl = input_vector[i];
			//printf("dbl %f | ", dbl);
			// Attempt conversion
			cd = dbl * AlpPrimitives<T>::EXP_ARR[state.v_exponent] * AlpPrimitives<T>::FRAC_ARR[state.v_factor];
			tmp_digit = NumberToInt64_WO(cd);
			//printf("tmpdigit %lld | ", (long long) tmp_digit);
			state.dig[i] = tmp_digit;
			orig = static_cast<T>(tmp_digit) * AlpConstants::FACT_ARR[state.v_factor] *
			       AlpPrimitives<T>::FRAC_ARR[state.v_exponent];
			tmp_dbl_arr[i] = orig;
		}
		printf("Finding Exceptions Positions\n");
		for (idx_t i {0}; i < n_values; i++) {
			auto l         = tmp_dbl_arr[i];
			auto r         = input_vector[i];
			auto compare_r = (l != r);
			tmp_index[pos] = i;
			pos += compare_r;
		}
		printf("Finding first encoded value\n");
		int64_t for_sure = 0;
		for (idx_t i {0}; i < n_values; i++) {
			if (i != tmp_index[i]) {
				for_sure = state.dig[i];
				break;
			}
		}
		printf("Building Exceptions\n");
		for (idx_t j {0}; j < pos; ++j) {
			idx_t i   = tmp_index[j];
			T dbl = input_vector[i];
			state.dig[i]       = for_sure;
			state.exceptions[exc_c] = dbl;
			state.exceptions_positions[exc_c] = i;
			//printf("EXCEPTION %f | ", dbl);
			exc_c  += 1;
		}
		state.exceptions_count = exc_c;
		printf("Found %d exceptions\n", state.exceptions_count);

		printf("Doing FFOR\n");
		// Analyze FFOR
		auto min = std::numeric_limits<int64_t>::max();
		auto max = std::numeric_limits<int64_t>::min();
		for (idx_t i {0}; i < n_values; ++i) {
			if (state.dig[i] < min) { min = state.dig[i]; }
			if (state.dig[i] > max) { max = state.dig[i]; }
		}
		uint64_t min_max_diff = (static_cast<uint64_t>(max) - static_cast<uint64_t>(min));

		auto* dig_u = reinterpret_cast<uint64_t*>(state.dig);
		auto const min_u = static_cast<uint64_t>(min);

		printf("min_u %d | ", min_u);

		// Subtract FOR
		if (!EMPTY){ // only if not analyze
			for (idx_t i = 0; i < n_values; i++) {
				//state.dig[i] -= min;
				//printf("ENCa %lld | ", (long long) dig_u[i]);
				dig_u[i] -= min_u;
				//printf("ENCb %lld | ", (long long) dig_u[i]);
			}
		}

		auto width = BitpackingPrimitives::MinimumBitWidth<uint64_t, false>(min_max_diff);
		auto bp_size = BitpackingPrimitives::GetRequiredSize(n_values, width);
		printf("BP SIZE %d\n", bp_size);
		printf("BW %d\n", width);
		printf("FOR %d\n", min);
		if (!EMPTY && width > 0){ // only if not analyze
			// state.encoded.reserve(bp_size); // reserving
			//data_ptr_t src = data_ptr_cast(state.dig);
			//data_ptr_t dst = data_ptr_cast(state.encoded.get());
			BitpackingPrimitives::PackBuffer<uint64_t, false>(
			    state.encoded,
			    dig_u,
			    n_values,
			    width);
		}
		state.bit_width = width; // in bits
		state.bp_size = bp_size; // in bytes
		state.frame_of_reference = min;

	}

};


template <class T>
struct AlpDecompression {
	static void Decompress(uint8_t *for_encoded, T* out, idx_t count, uint8_t v_factor, uint8_t v_exponent,
	                       uint16_t exceptions_count, T* exceptions, uint16_t* exceptions_positions,
	                       uint64_t frame_of_reference, uint8_t bit_width) {
		printf("Factor %d, Exponent %d", v_factor, v_exponent);
		uint64_t factor = AlpConstants::U_FACT_ARR[v_factor];
		T frac10 = AlpPrimitives<T>::FRAC_ARR[v_exponent];

		uint8_t for_decoded[AlpConstants::ALP_VECTOR_SIZE * 8] = {0};

		// TODO: Manage case of bit_width equal to 0
		if (bit_width > 0){
			BitpackingPrimitives::UnPackBuffer<uint64_t>(for_decoded, for_encoded, count, bit_width);
		}

		uint64_t *dig = reinterpret_cast<uint64_t*>(data_ptr_cast(for_decoded));

		// UNFOR
		for (idx_t i = 0; i < count; i++) {
			dig[i] += frame_of_reference;
		}

		for (idx_t i = 0; i < count; i++) {
			auto digit = dig[i];
			//printf("DECa %lld | ", (long long) digit);
			out[i] = static_cast<T>(static_cast<int64_t>(digit)) * factor * frac10;
			//printf("OUT %f | ", out[i]);
		}
		//printf("EXCEPTIONS COUNT DECOMPRESS %d", exceptions_count);
		for (idx_t i = 0; i < exceptions_count; i++) {
			//printf("EXCEPTION %f, %d | ", exceptions[i], exceptions_positions[i]);
			out[exceptions_positions[i]] = static_cast<T>(exceptions[i]);
		}
	}
};

} // namespace alp

} // namespace duckdb