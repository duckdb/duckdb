//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/alp/algorithm/alp.hpp"
#include "duckdb/storage/compression/alp/alp_constants.hpp"
#include "duckdb/storage/compression/alp/alp_utils.hpp"
#include "duckdb/storage/compression/patas/patas.hpp"
#include "duckdb/storage/table/column_data.hpp"

#include <cmath>

namespace duckdb {

template <class T>
struct AlpAnalyzeState : public AnalyzeState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::TYPE;

	explicit AlpAnalyzeState(const CompressionInfo &info) : AnalyzeState(info), state() {
	}

	idx_t total_bytes_used = 0;
	idx_t current_bytes_used_in_segment = 0;
	idx_t vectors_sampled_count = 0;
	idx_t total_values_count = 0;
	idx_t vectors_count = 0;
	vector<vector<T>> rowgroup_sample;
	vector<vector<T>> complete_vectors_sampled;
	alp::AlpCompressionState<T, true> state;

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
		    state.bp_size + state.exceptions_count * (sizeof(EXACT_TYPE) + AlpConstants::EXCEPTION_POSITION_SIZE) +
		    AlpConstants::EXPONENT_SIZE + AlpConstants::FACTOR_SIZE + AlpConstants::EXCEPTIONS_COUNT_SIZE +
		    AlpConstants::FOR_SIZE + AlpConstants::BIT_WIDTH_SIZE + AlpConstants::METADATA_POINTER_SIZE;
		return required_space;
	}

	void FlushVector() {
		current_bytes_used_in_segment += RequiredSpace();
		state.Reset();
	}

	// Check if we have enough space in the segment to hyphotetically store the compressed vector
	bool HasEnoughSpace() {
		idx_t bytes_to_be_used = AlignValue(current_bytes_used_in_segment + RequiredSpace());
		// We have enough space if the already used space + the required space for a new vector
		// does not exceed the space of the block - the segment header (the pointer to the metadata)
		return bytes_to_be_used <= (info.GetBlockSize() - AlpConstants::METADATA_POINTER_SIZE);
	}

	idx_t TotalUsedBytes() const {
		return AlignValue(total_bytes_used);
	}
};

template <class T>
unique_ptr<AnalyzeState> AlpInitAnalyze(ColumnData &col_data, PhysicalType type) {
	CompressionInfo info(col_data.GetBlockManager().GetBlockSize(), type);
	return make_uniq<AlpAnalyzeState<T>>(info);
}

/*
 * ALP Analyze step only pushes the needed samples to estimate the compression size in the finalize step
 */
template <class T>
bool AlpAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = (AlpAnalyzeState<T> &)state;
	bool must_skip_current_vector = alp::AlpUtils::MustSkipSamplingFromCurrentVector(
	    analyze_state.vectors_count, analyze_state.vectors_sampled_count, count);
	analyze_state.vectors_count += 1;
	analyze_state.total_values_count += count;
	if (must_skip_current_vector) {
		return true;
	}

	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<T>(vdata);

	alp::AlpSamplingParameters sampling_params = alp::AlpUtils::GetSamplingParameters(count);

	vector<uint16_t> current_vector_null_positions(sampling_params.n_lookup_values, 0);
	vector<T> current_vector_values(sampling_params.n_lookup_values, 0);
	vector<T> current_vector_sample(sampling_params.n_sampled_values, 0);

	// Storing the entire sampled vector
	//! We need to store the entire sampled vector to perform the 'analyze' compression in it
	idx_t nulls_idx = 0;
	// We optimize by doing a different loop when there are no nulls
	if (vdata.validity.AllValid()) {
		for (idx_t i = 0; i < sampling_params.n_lookup_values; i++) {
			auto idx = vdata.sel->get_index(i);
			T value = data[idx];
			current_vector_values[i] = value;
		}
	} else {
		for (idx_t i = 0; i < sampling_params.n_lookup_values; i++) {
			auto idx = vdata.sel->get_index(i);
			T value = data[idx];
			//! We resolve null values with a predicated comparison
			bool is_null = !vdata.validity.RowIsValid(idx);
			current_vector_null_positions[nulls_idx] = UnsafeNumericCast<uint16_t>(i);
			nulls_idx += is_null;
			current_vector_values[i] = value;
		}
		alp::AlpUtils::FindAndReplaceNullsInVector<T>(current_vector_values.data(),
		                                              current_vector_null_positions.data(),
		                                              sampling_params.n_lookup_values, nulls_idx);
	}

	// Storing the sample of that vector
	idx_t sample_idx = 0;
	for (idx_t i = 0; i < sampling_params.n_lookup_values; i += sampling_params.n_sampled_increments) {
		current_vector_sample[sample_idx] = current_vector_values[i];
		sample_idx++;
	}
	D_ASSERT(sample_idx == sampling_params.n_sampled_values);

	//! A std::move is needed to avoid a copy of the pushed vector
	analyze_state.complete_vectors_sampled.push_back(std::move(current_vector_values));
	analyze_state.rowgroup_sample.push_back(std::move(current_vector_sample));
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
	alp::AlpCompression<T, true>::FindTopKCombinations(analyze_state.rowgroup_sample, analyze_state.state);

	// Encode the entire sampled vectors to estimate a compression size
	idx_t compressed_values = 0;
	for (auto &vector_to_compress : analyze_state.complete_vectors_sampled) {
		alp::AlpCompression<T, true>::Compress(vector_to_compress.data(), vector_to_compress.size(),
		                                       analyze_state.state);
		if (!analyze_state.HasEnoughSpace()) {
			analyze_state.FlushSegment();
		}
		analyze_state.FlushVector();
		compressed_values += vector_to_compress.size();
	}

	// Flush last unfinished segment
	analyze_state.FlushSegment();

	if (compressed_values == 0) {
		return DConstants::INVALID_INDEX;
	}

	// We estimate the size by taking into account the portion of the values we took
	const auto factor_of_sampling = analyze_state.total_values_count / compressed_values;
	const auto final_analyze_size = analyze_state.TotalUsedBytes() * factor_of_sampling;
	return final_analyze_size; // return size of data in bytes
}

} // namespace duckdb
