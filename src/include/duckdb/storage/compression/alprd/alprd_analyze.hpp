//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/alprd/alprd.hpp"
#include "duckdb/storage/compression/alprd/alprd_constants.hpp"
#include "duckdb/storage/compression/alp/alp_constants.hpp"

#include <cmath>

namespace duckdb {

template <class T>
struct AlpRDAnalyzeState : public AnalyzeState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	AlpRDAnalyzeState() : state((void *)this) {
	}

	idx_t vectors_count = 0;
	idx_t total_values_count = 0;
	idx_t vectors_sampled_count = 0;
	vector<EXACT_TYPE> rowgroup_sample;
	AlpRDState<T, true> state;
};

template <class T>
unique_ptr<AnalyzeState> AlpRDInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<AlpRDAnalyzeState<T>>();
}

/*
 * ALPRD Analyze step only pushes the needed samples to estimate the compression size in the finalize step
 */
template <class T>
bool AlpRDAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	using EXACT_TYPE = typename FloatingToExact<T>::type;
	auto &analyze_state = (AlpRDAnalyzeState<T> &)state;
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<T>(vdata);

	//! We sample equidistant vectors; to do this we skip a fixed values of vectors
	bool must_select_rowgroup_samples = (analyze_state.vectors_count % AlpConstants::RG_SAMPLES_DUCKDB_JUMP) == 0;
	analyze_state.vectors_count += 1;
	analyze_state.total_values_count += count;

	//! If we are not in the correct jump, we do not take sample from this vector
	if (!must_select_rowgroup_samples) {
		return true;
	}

	//! We do not take samples of non-complete duckdb vectors (usually the last one)
	//! Except in the case of too little data
	if (count < AlpConstants::SAMPLES_PER_VECTOR && analyze_state.vectors_sampled_count != 0) {
		return true;
	}

	uint32_t n_lookup_values = MinValue(count, (idx_t)AlpConstants::ALP_VECTOR_SIZE);
	//! We sample equidistant values within a vector; to do this we jump a fixed number of values
	uint32_t n_sampled_increments =
	    MaxValue(1, (int)std::ceil((double)n_lookup_values / AlpConstants::SAMPLES_PER_VECTOR));
	uint32_t n_sampled_values = std::ceil((double)n_lookup_values / n_sampled_increments);

	vector<EXACT_TYPE> current_vector_sample(n_sampled_values, 0);
	vector<uint16_t> current_vector_null_positions(n_lookup_values, 0);
	EXACT_TYPE a_non_null_value = 0;
	idx_t nulls_idx = 0;

	// Storing the sample of that vector
	idx_t sample_idx = 0;
	for (idx_t i = 0; i < n_lookup_values; i += n_sampled_increments) {
		auto idx = vdata.sel->get_index(i);
		EXACT_TYPE value = Load<EXACT_TYPE>(const_data_ptr_cast(&data[idx]));
		current_vector_sample[sample_idx] = value;
		//! We resolve null values with a predicated comparison
		bool is_null = !vdata.validity.RowIsValid(idx);
		current_vector_null_positions[nulls_idx] = sample_idx;
		nulls_idx += is_null;
		sample_idx++;
	}
	D_ASSERT(sample_idx == n_sampled_values);

	// Finding the first non-null value
	idx_t tmp_null_idx = 0;
	for (idx_t i = 0; i < n_sampled_values; i++) {
		if (i != current_vector_null_positions[tmp_null_idx]) {
			a_non_null_value = current_vector_sample[i];
			break;
		}
		tmp_null_idx += 1;
	}

	// Replacing that first non-null value on the vector
	for (idx_t i = 0; i < nulls_idx; i++) {
		uint16_t null_value_pos = current_vector_null_positions[i];
		current_vector_sample[null_value_pos] = a_non_null_value;
	}

	// Pushing the sampled vector samples into the rowgroup samples
	for (auto &value : current_vector_sample) {
		analyze_state.rowgroup_sample.push_back(value);
	}

	analyze_state.vectors_sampled_count++;
	return true;
}

/*
 * Estimate the compression size of ALPRD using the taken samples
 */
template <class T>
idx_t AlpRDFinalAnalyze(AnalyzeState &state) {
	auto &analyze_state = (AlpRDAnalyzeState<T> &)state;
	double factor_of_sampling = analyze_state.rowgroup_sample.size() / analyze_state.total_values_count;

	// Finding which is the best dictionary for the sample
	double estimated_bits_per_value = alp::AlpRDCompression<T, true>::FindBestDictionary(analyze_state.rowgroup_sample,
	                                                                                     analyze_state.state.alp_state);
	double estimated_compressed_bits = estimated_bits_per_value * analyze_state.rowgroup_sample.size();
	double estimed_compressed_bytes = estimated_compressed_bits / 8;

	//! Overhead per segment: [Pointer to metadata + right bitwidth] + Dictionary Size
	double per_segment_overhead = AlpRDConstants::HEADER_SIZE + AlpRDConstants::DICTIONARY_SIZE_BYTES;

	//! Overhead per vector: Pointer to data + Exceptions count
	double per_vector_overhead = AlpRDConstants::METADATA_POINTER_SIZE + AlpRDConstants::EXCEPTIONS_COUNT_SIZE;

	uint32_t n_vectors = std::ceil((double)analyze_state.total_values_count / AlpRDConstants::ALP_VECTOR_SIZE);

	auto estimated_size = (estimed_compressed_bytes * factor_of_sampling) + (n_vectors * per_vector_overhead);
	uint32_t estimated_n_blocks = std::ceil(estimated_size / (Storage::BLOCK_SIZE - per_segment_overhead));

	auto final_analyze_size = estimated_size + (estimated_n_blocks * per_segment_overhead);
	return final_analyze_size;
}

} // namespace duckdb
