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

	idx_t processed_groups = 0;
	idx_t vector_idx = 0;
	idx_t total_bytes_used = 0;
	idx_t current_bytes_used = 0;
	idx_t vectors_sampled = 0;
	bool have_valid_row = false;
	RandomEngine random_engine;
	vector<T> rg_sample;
	vector<T> vectors_samples;
	vector<T> all_data;
	AlpState<T, true> state; // This has true because analyze won't write

public:

	void FlushSegment() {
		// Add the size of the Pointer to first metadata
		total_bytes_used += current_bytes_used + sizeof(uint32_t);
		current_bytes_used = 0;
	}

	idx_t RequiredSpace() const {
		idx_t required_space =
		    state.alp_state.bp_size + // FOR ENCODED
		    state.alp_state.exceptions_count * (sizeof(EXACT_TYPE) + 2) + // Exceptions
		    sizeof(uint8_t) + // E
		    sizeof(uint8_t) +  // F
		    sizeof(uint16_t) + // Exceptions Count
			sizeof(uint64_t) + // FOR
		    sizeof(uint8_t) + // BW
		    sizeof(uint32_t); // Pointer to next group
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
		return bytes_to_be_used <= (Storage::BLOCK_SIZE - sizeof(uint32_t)); // Storage block minus first pointer to metadata
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

	bool select_rg_samples = (analyze_state.vector_idx % AlpConstants::RG_SAMPLES_VECTOR_JUMP) == 0;

	analyze_state.vector_idx++;

	if (!select_rg_samples) {
		return true;
	}

	//! We do not take samples of non-complete vectors (usually the last one)
	if (count < AlpConstants::SAMPLES_PER_VECTOR){
		return true;
	}

	//printf("Hi %d\n", count);
	// printf("Vector IDX %ld\n", analyze_state.vector_idx);
	// printf("Vector Sampled %d\n", 	analyze_state.vectors_sampled);

	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		// TODO: Manage Null values
		 if (!vdata.validity.RowIsValid(idx)) {
			 // printf("NA %ld - ", i);
			 continue;
		 }
		 // TODO
		 // So we have a problem here because for each vector I am pushing 2048, but each vector in ALP is 1024
		 // So I end up sampling more than what I should
		 analyze_state.all_data.push_back(data[idx]); // I will use this later to test the analyze
		// analyze_state.have_valid_row = true;
		if (i % ( count / AlpConstants::SAMPLES_PER_VECTOR) == 0) { // This will ensure I always take 32 samples per vector
			analyze_state.rg_sample.push_back(data[idx]);
		}
	}
	analyze_state.vectors_sampled++;
	return true;
}

// Encode the data
template <class T>
idx_t AlpFinalAnalyze(AnalyzeState &state) {
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	auto &analyze_state = (AlpAnalyzeState<T> &)state;
	// For now I will not use 2nd level sampling neither optimize the analyze step
	// printf("Starting First Level Sampling with %d saples from %d vectors\n", analyze_state.rg_sample.size(), analyze_state.vectors_sampled);
	alp::AlpCompression<T, true>::FindTopNCombinations(
	    analyze_state.rg_sample,
	    analyze_state.vectors_sampled,
	    analyze_state.state.alp_state);
	// printf("Finished First Level Sampling\n");
	printf("BEST COMB %d - %d\n", analyze_state.state.alp_state.combinations[0].first,
	       analyze_state.state.alp_state.combinations[0].second);
	vector<T> vector_to_compress(AlpConstants::ALP_VECTOR_SIZE);
	printf("Here\n");
	idx_t c = 0;
	for (auto &value : analyze_state.all_data){
		vector_to_compress[c] = value;
		c++;
		if (c % AlpConstants::ALP_VECTOR_SIZE == 0){
			alp::AlpCompression<T, true>::Compress(vector_to_compress, analyze_state.state.alp_state);
			if (!analyze_state.HasEnoughSpace()) {
				printf("\n\nNOT ENOUGH SPACE; FLUSHING SEGMENT \n");
				analyze_state.FlushSegment();
			}
			analyze_state.FlushGroup();
			c = 0;
		}
	}
	//! We ignore heterogeneous last iteration

	// Flush last segment (Finalize in compress)
	analyze_state.FlushSegment();

	// We estimate the size by taking into account the portion of vectors we took
	const auto factor_of_sampling = analyze_state.vector_idx / analyze_state.vectors_sampled;
	const auto final_analyze_size = analyze_state.TotalUsedBytes() * factor_of_sampling;
	// printf("Finished Analyzing | Total size in bytes %d:\n", final_analyze_size);
	// printf("%d Vectors\n", analyze_state.vector_idx);
	// printf("Real %d | Mult Factor %d\n", analyze_state.TotalUsedBytes(), factor_of_sampling);
	// printf(" =========================== FINISH ANALYZE ========= \n\n");
	return final_analyze_size; // return size of data in bytes
}

} // namespace duckdb
