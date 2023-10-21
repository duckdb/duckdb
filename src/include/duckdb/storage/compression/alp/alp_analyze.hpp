//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alp/alp.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/alp/shared.hpp"
#include "duckdb/common/random_engine.hpp"

namespace duckdb {

template <class T>
struct AlpAnalyzeState : public AnalyzeState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	AlpAnalyzeState() : state((void *)this) {

	}

	idx_t total_bytes_used = 0;
	idx_t current_bytes_used = 0;
	idx_t vectors_sampled_idx = 0;
	idx_t total_analyze_values = 0;
	idx_t vector_idx = 0;
	RandomEngine random_engine;
	vector<vector<T>> rg_sample;
	vector<vector<T>> vectors_sampled;
	AlpState<T, true> state; // This has true because analyze won't write

public:

	void FlushSegment() {
		// Add the size of the Pointer to first metadata
		total_bytes_used += current_bytes_used + AlpConstants::METADATA_POINTER_SIZE;
		current_bytes_used = 0;
	}

	idx_t RequiredSpace() const {
		idx_t required_space =
		    state.alp_state.bp_size + // FOR ENCODED
		    state.alp_state.exceptions_count * (sizeof(EXACT_TYPE) + AlpConstants::EXCEPTION_POSITION_SIZE) + // Exceptions
		    AlpConstants::EXPONENT_SIZE + // E
		    AlpConstants::FACTOR_SIZE +  // F
		    AlpConstants::EXCEPTIONS_COUNT_SIZE + // Exceptions Count
		    AlpConstants::FOR_SIZE + // FOR
		    AlpConstants::BW_SIZE + // BW
		    AlpConstants::METADATA_POINTER_SIZE; // Pointer to next group
		return required_space;
	}

	void FlushGroup() {
		current_bytes_used += RequiredSpace();
		state.alp_state.Reset();
	}

	idx_t UsedSpace() const {
		return current_bytes_used;
	}

	bool HasEnoughSpace() {
		idx_t bytes_to_be_used = AlignValue(UsedSpace() + RequiredSpace());
		return bytes_to_be_used <= (Storage::BLOCK_SIZE - AlpConstants::METADATA_POINTER_SIZE); // Storage block minus first pointer to metadata
	}

	idx_t TotalUsedBytes() const {
		return AlignValue(total_bytes_used);
	}
};

template <class T>
unique_ptr<AnalyzeState> AlpInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_uniq<AlpAnalyzeState<T>>();
}

template <class T>
// Takes the samples per rowgroup and per vector
bool AlpAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	auto &analyze_state = (AlpAnalyzeState<T> &)state;
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);
	auto data = UnifiedVectorFormat::GetData<T>(vdata);

	printf("JUMP MATE JUMP %d", AlpConstants::RG_SAMPLES_DUCKDB_JUMP);

	bool select_rg_samples = (analyze_state.vector_idx % AlpConstants::RG_SAMPLES_DUCKDB_JUMP) == 0;
	analyze_state.vector_idx += 1;
	analyze_state.total_analyze_values += count;

	// If we are not in the correct jump, we do not take sample from this vector
	if (!select_rg_samples) {
		return true;
	}

	//! We do not take samples of non-complete duckdb vectors (usually the last one)
	//! Execpt for an special case in which there is too little data (second case)
	if (count < AlpConstants::SAMPLES_PER_VECTOR && analyze_state.vectors_sampled_idx != 0){
		return true;
	}

	//printf("Hi %d\n", count);
	// printf("Vector IDX %ld\n", analyze_state.vector_idx);
	// printf("Vector Sampled %d\n", 	analyze_state.vectors_sampled);

	uint32_t n_lookup_values = MinValue(count, (idx_t) AlpConstants::ALP_VECTOR_SIZE);
	uint32_t n_sampled_increments = MaxValue(1, (int) ceil((double) n_lookup_values / AlpConstants::SAMPLES_PER_VECTOR));
	uint32_t n_sampled_values = ceil((double) n_lookup_values / n_sampled_increments);
	T a_non_null_value = 0;
	idx_t nulls_idx = 0;

	vector<uint16_t> null_positions(n_lookup_values, 0);
	vector<T> current_vector_values(n_lookup_values, 0);
	vector<T> current_vector_sample(n_sampled_values, 0);

	// Storing the entire vector
	for (idx_t i = 0; i < n_lookup_values; i++) {
		auto idx = vdata.sel->get_index(i);
		T value = data[idx];
		bool is_valid = !vdata.validity.RowIsValid(idx);
		null_positions[nulls_idx] = i;
		nulls_idx += is_valid;
		current_vector_values[i] = value;
	}

	// Finding the first non-null value
	idx_t tmp_null_idx = 0;
	for (idx_t i = 0; i < n_lookup_values; i++){
		if (i != null_positions[tmp_null_idx]){
			a_non_null_value = current_vector_values[i];
			break;
		}
		tmp_null_idx += 1;
	}

	// Replacing it on the vector
	for (idx_t j = 0; j < nulls_idx; j++){
		uint16_t null_value_pos = null_positions[j];
		current_vector_values[null_value_pos] = a_non_null_value;
	}

	// Storing the sample of that vector
	idx_t v_i = 0;
	for (idx_t i = 0; i < n_lookup_values; i+= n_sampled_increments){
		current_vector_sample.push_back(current_vector_values[i]);
		v_i++;
	}
	D_ASSERT(v_i == n_sampled_values);

	analyze_state.vectors_sampled.push_back(current_vector_values);
	analyze_state.rg_sample.push_back(current_vector_sample);
	analyze_state.vectors_sampled_idx++;
	return true;
}

// Encode the data
template <class T>
idx_t AlpFinalAnalyze(AnalyzeState &state) {

	auto &analyze_state = (AlpAnalyzeState<T> &)state;
	// For now I will not use 2nd level sampling neither optimize the analyze step
	// printf("Starting First Level Sampling with %d saples from %d vectors\n", analyze_state.rg_sample.size(), analyze_state.vectors_sampled);
	alp::AlpCompression<T, true>::FindTopNCombinations(analyze_state.rg_sample,analyze_state.state.alp_state);
	// printf("Finished First Level Sampling\n");
	printf("BEST COMB %d - %d\n", analyze_state.state.alp_state.combinations[0].first,
	       analyze_state.state.alp_state.combinations[0].second);
	printf("Here\n");
	idx_t compressed_values = 0;
	for (auto &vector_to_compress : analyze_state.vectors_sampled){
			alp::AlpCompression<T, true>::Compress(
		    vector_to_compress,
		    vector_to_compress.size(),
		    analyze_state.state.alp_state);
			if (!analyze_state.HasEnoughSpace()) {
				printf("\n\nNOT ENOUGH SPACE; FLUSHING SEGMENT \n");
				analyze_state.FlushSegment();
			}
			analyze_state.FlushGroup();
		    compressed_values += vector_to_compress.size();
	}

	// Flush last segment (Finalize in compress)
	analyze_state.FlushSegment();

	// We estimate the size by taking into account the portion of values we took
	const auto factor_of_sampling = analyze_state.total_analyze_values / compressed_values;
	const auto final_analyze_size = analyze_state.TotalUsedBytes() * factor_of_sampling;
	printf("Finished Analyzing | Total size in bytes %d:\n", final_analyze_size);
	// printf("%d Vectors\n", analyze_state.vector_idx);
	// printf("Real %d | Mult Factor %d\n", analyze_state.TotalUsedBytes(), factor_of_sampling);
	printf(" =========================== FINISH ANALYZE ========= \n\n");
	return final_analyze_size; // return size of data in bytes
}

} // namespace duckdb
