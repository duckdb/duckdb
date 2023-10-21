//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alprd/alprd.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/alprd/shared.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

template <class T>
struct AlpRDAnalyzeState : public AnalyzeState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	AlpRDAnalyzeState() : state((void *)this) {

	}

	idx_t vector_idx = 0;
	idx_t total_values_count = 0;
	idx_t vectors_sampled_idx = 0;
	RandomEngine random_engine;
	vector<EXACT_TYPE> rg_sample;
	AlpRDState<T, true> state; // This has true because analyze won't write

};

template <class T>
unique_ptr<AnalyzeState> AlpRDInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<AlpRDAnalyzeState<T>>();
}

template <class T>
// Takes the samples per rowgroup and per vector
bool AlpRDAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	printf("INIT Analyze\n");
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	auto &analyze_state = (AlpRDAnalyzeState<T> &)state;
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<T>(vdata);

	bool select_rg_samples = (analyze_state.vector_idx % AlpConstants::RG_SAMPLES_DUCKDB_JUMP) == 0;
	analyze_state.vector_idx += 1;
	analyze_state.total_values_count += count;

	if (!select_rg_samples) {
		return true;
	}

	//! We do not take samples of non-complete duckdb vectors (usually the last one)
	//! Execpt for an special case in which there is too little data (second case)
	if (count < AlpConstants::SAMPLES_PER_VECTOR && analyze_state.vectors_sampled_idx != 0){
		return true;
	}

	uint32_t n_lookup_values = MinValue(count, (idx_t) AlpConstants::ALP_VECTOR_SIZE);
	uint32_t n_sampled_increments = MaxValue(1, (int) ceil((double) n_lookup_values / AlpConstants::SAMPLES_PER_VECTOR));
	uint32_t n_sampled_values = ceil((double) n_lookup_values / n_sampled_increments);
	EXACT_TYPE a_non_null_value = 0;
	idx_t nulls_idx = 0;

	vector<uint16_t> null_positions(n_lookup_values, 0);
	vector<EXACT_TYPE> current_vector_sample(n_sampled_values, 0);

	printf("N Total Values %d ====\n", count);
	printf("N Lookup Values %d ====\n", n_lookup_values);
	printf("N Sampled Increments %d ====\n", n_sampled_increments);
	printf("N Sampled Values %d ====\n", n_sampled_values);

	// Storing the sample of that vector
	idx_t v_i = 0;
	for (idx_t i = 0; i < n_lookup_values; i+= n_sampled_increments){
		auto idx = vdata.sel->get_index(i);
		EXACT_TYPE value = Load<EXACT_TYPE>(const_data_ptr_cast(&data[idx]));
		current_vector_sample[v_i] = value;

		bool is_valid = !vdata.validity.RowIsValid(idx);
		null_positions[nulls_idx] = v_i;
		nulls_idx += is_valid;

		v_i++;
	}
	printf("Actual Sampled Values %d ====\n", v_i);
	D_ASSERT(v_i == n_sampled_values);

	// Finding the first non-null value
	idx_t tmp_null_idx = 0;
	for (idx_t i = 0; i < n_sampled_values; i++){
		if (i != null_positions[tmp_null_idx]){
			a_non_null_value = current_vector_sample[i];
			break;
		}
		tmp_null_idx += 1;
	}

	// Replacing it on the vector
	for (idx_t j = 0; j < nulls_idx; j++){
		uint16_t null_value_pos = null_positions[j];
		current_vector_sample[null_value_pos] = a_non_null_value;
	}

	for (auto &value : current_vector_sample){
		analyze_state.rg_sample.push_back(value);
	}

	analyze_state.vectors_sampled_idx++;
	return true;
}

// Size can be completely estimated without a per-vector processing
template <class T>
idx_t AlpRDFinalAnalyze(AnalyzeState &state) {
	printf("Final Analyze\n");
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	auto &analyze_state = (AlpRDAnalyzeState<T> &)state;
	double factor_of_sampling = analyze_state.rg_sample.size() / analyze_state.total_values_count;

	double estimated_bits_per_value = alp::AlpRDCompression<T, true>::FindBestDictionary(
	    analyze_state.rg_sample,
	    analyze_state.state.alp_state);
	double estimated_compressed_size = estimated_bits_per_value * analyze_state.rg_sample.size();
	double estimed_compressed_bytes = estimated_compressed_size / 8;

	double per_segment_overhead = AlpRDConstants::HEADER_SIZE + AlpRDConstants::DICTIONARY_SIZE_BYTES;// [Pointer to metadata + r_bw] + Dictionary Size
	double per_vector_overhead = AlpRDConstants::METADATA_POINTER_SIZE + AlpRDConstants::EXCEPTIONS_COUNT_SIZE; // Pointer to data + Exceptions count

	auto num_vectors = analyze_state.total_values_count / AlpRDConstants::ALP_VECTOR_SIZE;

	auto estimated_base_size = (estimed_compressed_bytes * factor_of_sampling) +
	                           (num_vectors * per_vector_overhead);
	auto num_blocks = estimated_base_size / (Storage::BLOCK_SIZE - per_segment_overhead);

	auto final_analyze_size = estimated_base_size +
	                          (num_blocks * per_segment_overhead);
	printf("Final Analyze END ====\n");
	return final_analyze_size; // return size of data in bytes
}

} // namespace duckdb
