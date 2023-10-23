//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/alp/alp.hpp"
#include "duckdb/storage/compression/alp/shared.hpp"
#include "duckdb/storage/compression/patas/patas.hpp"

namespace duckdb {

template <class T>
struct AlpAnalyzeState : public AnalyzeState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	AlpAnalyzeState() : state((void *)this) {
	}

	idx_t total_bytes_used = 0;
	idx_t current_bytes_used_in_segment = 0;
	idx_t vectors_sampled_count = 0;
	idx_t total_values_count = 0;
	idx_t vectors_count = 0;
	vector<vector<T>> rowgroup_sample;
	vector<vector<T>> complete_vectors_sampled;
	AlpState<T, true> state;

public:
	// Returns the required space to hyphotetically store the compressed segment
	void FlushSegment() {
		// We add the size of the segment header (the pointer to the metadata)
		total_bytes_used += current_bytes_used_in_segment + AlpConstants::METADATA_POINTER_SIZE;
		current_bytes_used_in_segment = 0;
	}

	// Returns the required space to hyphotetically store the compressed vector
	idx_t RequiredSpace() const {
		idx_t required_space =
		    state.alp_state.bp_size +
		    state.alp_state.exceptions_count * (sizeof(EXACT_TYPE) + AlpConstants::EXCEPTION_POSITION_SIZE) +
		    AlpConstants::EXPONENT_SIZE +
		    AlpConstants::FACTOR_SIZE +
		    AlpConstants::EXCEPTIONS_COUNT_SIZE +
		    AlpConstants::FOR_SIZE +
		    AlpConstants::BW_SIZE +
		    AlpConstants::METADATA_POINTER_SIZE;
		return required_space;
	}

	void FlushVector() {
		current_bytes_used_in_segment += RequiredSpace();
		state.alp_state.Reset();
	}

	// Check if we have enough space in the segment to hyphotetically store the compressed vector
	bool HasEnoughSpace() {
		idx_t bytes_to_be_used = AlignValue(current_bytes_used_in_segment + RequiredSpace());
		// We have enough space if the already used space + the required space for a new vector
		// does not exceed the space of the block - the segment header (the pointer to the metadata)
		return bytes_to_be_used <= (Storage::BLOCK_SIZE - AlpConstants::METADATA_POINTER_SIZE);
	}

	idx_t TotalUsedBytes() const {
		return AlignValue(total_bytes_used);
	}
};

template <class T>
unique_ptr<AnalyzeState> AlpInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<AlpAnalyzeState<T>>();
}

/*
 * ALP Analyze step only pushes the needed samples to estimate the compression size in the finalize step
 */
template <class T>
bool AlpAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = (AlpAnalyzeState<T> &)state;
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
	if (count < AlpConstants::SAMPLES_PER_VECTOR && analyze_state.vectors_sampled_count != 0){
		return true;
	}


	uint32_t n_lookup_values = MinValue(count, (idx_t) AlpConstants::ALP_VECTOR_SIZE);
	//! We sample equidistant values within a vector; to do this we jump a fixed number of values
	uint32_t n_sampled_increments =
	    MaxValue(1, (int) ceil((double) n_lookup_values / AlpConstants::SAMPLES_PER_VECTOR));
	uint32_t n_sampled_values = ceil((double) n_lookup_values / n_sampled_increments);

	T a_non_null_value = 0;
	idx_t nulls_idx = 0;

	vector<uint16_t> current_vector_null_positions(n_lookup_values, 0);
	vector<T> current_vector_values(n_lookup_values, 0);
	vector<T> current_vector_sample(n_sampled_values, 0);

	// Storing the entire sampled vector
	//! We need to store the entire sampled vector to perform the 'analyze' compression in it
	for (idx_t i = 0; i < n_lookup_values; i++) {
		auto idx = vdata.sel->get_index(i);
		T value = data[idx];
		//! We resolve null values with a predicated comparison
		bool is_null = !vdata.validity.RowIsValid(idx);
		current_vector_null_positions[nulls_idx] = i;
		nulls_idx += is_null;
		current_vector_values[i] = value;
	}

	// Finding the first non-null value
	idx_t tmp_null_idx = 0;
	for (idx_t i = 0; i < n_lookup_values; i++){
		if (i != current_vector_null_positions[tmp_null_idx]){
			a_non_null_value = current_vector_values[i];
			break;
		}
		tmp_null_idx += 1;
	}

	// Replacing that first non-null value on the vector
	for (idx_t i = 0; i < nulls_idx; i++){
		uint16_t null_value_pos = current_vector_null_positions[i];
		current_vector_values[null_value_pos] = a_non_null_value;
	}

	// Storing the sample of that vector
	idx_t sample_idx = 0;
	for (idx_t i = 0; i < n_lookup_values; i+= n_sampled_increments){
		current_vector_sample[sample_idx] = current_vector_values[i];
		sample_idx++;
	}
	D_ASSERT(sample_idx == n_sampled_values);

	analyze_state.complete_vectors_sampled.push_back(current_vector_values);
	analyze_state.rowgroup_sample.push_back(current_vector_sample);
	analyze_state.vectors_sampled_count++;
	return true;
}


/*
 * Estimate the compression size of ALP using the taken samples
 */
template <class T>
idx_t AlpFinalAnalyze(AnalyzeState &state) {
	auto &analyze_state = (AlpAnalyzeState<T> &)state;

	// Finding the Top K combinations of Exponent and Factor
	alp::AlpCompression<T, true>::FindTopKCombinations(
	    analyze_state.rowgroup_sample, analyze_state.state.alp_state);

	// Encode the entire sampled vectors to estimate a compression size
	idx_t compressed_values = 0;
	for (auto &vector_to_compress : analyze_state.complete_vectors_sampled){
			alp::AlpCompression<T, true>::Compress(
		    vector_to_compress,
		    vector_to_compress.size(),
		    analyze_state.state.alp_state);
			if (!analyze_state.HasEnoughSpace()) {
				analyze_state.FlushSegment();
			}
		    analyze_state.FlushVector();
		    compressed_values += vector_to_compress.size();
	}

	// Flush last unfinished segment
	analyze_state.FlushSegment();

	// We estimate the size by taking into account the portion of the values we took
	const auto factor_of_sampling = analyze_state.total_values_count / compressed_values;
	const auto final_analyze_size = analyze_state.TotalUsedBytes() * factor_of_sampling;
	return final_analyze_size; // return size of data in bytes
}

} // namespace duckdb
