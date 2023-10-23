//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_compress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alp/alp.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/alp/alp_analyze.hpp"

#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include <functional>

namespace duckdb {

template <class T>
struct AlpCompressionState : public CompressionState {

public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;
	explicit AlpCompressionState(ColumnDataCheckpointer &checkpointer, AlpAnalyzeState<T> *analyze_state)
	    : checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ALP)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = (void *)this;
		input_vector = vector<T>(AlpConstants::ALP_VECTOR_SIZE, 0);
		vector_null_positions = vector<uint16_t>(AlpConstants::ALP_VECTOR_SIZE, 0);

		//! Combinations found on the analyze step are needed for compression
		state.alp_state.best_k_combinations = analyze_state->state.alp_state.best_k_combinations;

	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	idx_t vector_idx = 0;
	idx_t nulls_idx = 0;
	idx_t vectors_flushed = 0;
	idx_t data_bytes_used = 0;

	data_ptr_t data_ptr; // Pointer to next free spot in segment;
	data_ptr_t metadata_ptr; // Reverse pointer to the next free spot for the metadata; used in decoding to SKIP vectors
	uint32_t next_vector_byte_index_start = AlpConstants::HEADER_SIZE;

	vector<T> input_vector;
	vector<uint16_t> vector_null_positions;

	AlpState<T, false> state;

public:

	// Returns the space currently used in the segment (in bytes)
	idx_t UsedSpace() const {
		return AlpConstants::METADATA_POINTER_SIZE + data_bytes_used;
	}

	// Returns the required space to store the newly compressed vector
	idx_t RequiredSpace() {
		idx_t required_space =
		    state.alp_state.bp_size +
		    (state.alp_state.exceptions_count * (sizeof(EXACT_TYPE) + AlpConstants::EXCEPTION_POSITION_SIZE)) +
		    AlpConstants::EXPONENT_SIZE +
		    AlpConstants::FACTOR_SIZE +
		    AlpConstants::EXCEPTIONS_COUNT_SIZE +
		    AlpConstants::FOR_SIZE +
		    AlpConstants::BW_SIZE;
		return required_space;
	}


	bool HasEnoughSpace() {
		//! If [start of block + used space + required space] is more than whats left (current position
		//! of metadata pointer - the size of a new metadata pointer)
		if ((handle.Ptr() + AlignValue(UsedSpace() + RequiredSpace())) >=
		    (metadata_ptr - AlpConstants::METADATA_POINTER_SIZE)){
			return false;
		}
		return true;
	}

	void ResetVector(){
		state.alp_state.Reset();
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		current_segment = std::move(compressed_segment);
		current_segment->function = function;
		auto &buffer_manager = BufferManager::GetBufferManager(current_segment->db);
		handle = buffer_manager.Pin(current_segment->block);

		// Pointer to the start of the compressed data
		data_ptr = handle.Ptr() + current_segment->GetBlockOffset() + AlpConstants::HEADER_SIZE;
		// Pointer to the start of the Metadata
		metadata_ptr = handle.Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE;

		next_vector_byte_index_start = AlpConstants::HEADER_SIZE;
	}

	// Replace NULL values with the first Non-NULL value on the vector
	void ReplaceNullsInVector() {
		// Finding the first non-null value
		idx_t tmp_null_idx = 0;
		T a_non_null_value = 0;
		for (idx_t i = 0; i < vector_idx; i++){
			if (i != vector_null_positions[tmp_null_idx]){
				a_non_null_value = input_vector[i];
				break;
			}
			tmp_null_idx += 1;
		}
		// Replacing that first non-null value on the vector
		for (idx_t j = 0; j < nulls_idx; j++){
			uint16_t null_value_pos = vector_null_positions[j];
			input_vector[null_value_pos] = a_non_null_value;
		}
	}

	void CompressVector(){
		ReplaceNullsInVector();
		alp::AlpCompression<T, false>::Compress(input_vector, vector_idx, state.alp_state);
		//! Check if the compressed vector fits on current segment
		if (!HasEnoughSpace()){
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}

		if (vector_idx != nulls_idx){ //! At least there is one valid value in the vector
			for (idx_t i = 0; i < vector_idx; i++){
				NumericStats::Update<T>(current_segment->stats.statistics, input_vector[i]);
			}
		}
		current_segment->count += vector_idx;
		FlushVector();
	}

	// Stores the vector and its metadata
	void FlushVector(){
		Store<uint8_t>(state.alp_state.vector_exponent,data_ptr);
		data_ptr += AlpConstants::EXPONENT_SIZE;

		Store<uint8_t>(state.alp_state.vector_factor,data_ptr);
		data_ptr += AlpConstants::FACTOR_SIZE;

		Store<uint16_t>(state.alp_state.exceptions_count ,data_ptr);
		data_ptr += AlpConstants::EXCEPTIONS_COUNT_SIZE;

		Store<uint64_t>(state.alp_state.frame_of_reference ,data_ptr);
		data_ptr += AlpConstants::FOR_SIZE;

		Store<uint8_t>(state.alp_state.bit_width ,data_ptr);
		data_ptr += AlpConstants::BW_SIZE;

		memcpy((void *) data_ptr, (void*) state.alp_state.values_encoded, state.alp_state.bp_size);
		data_ptr += state.alp_state.bp_size;

		if (state.alp_state.exceptions_count > 0){
			memcpy((void *) data_ptr, (void*) state.alp_state.exceptions,
			       sizeof(EXACT_TYPE) * state.alp_state.exceptions_count);
			data_ptr += sizeof(EXACT_TYPE) * state.alp_state.exceptions_count;
			memcpy((void *) data_ptr, (void*) state.alp_state.exceptions_positions,
			       AlpConstants::EXCEPTION_POSITION_SIZE * state.alp_state.exceptions_count);
			data_ptr += AlpConstants::EXCEPTION_POSITION_SIZE * state.alp_state.exceptions_count;
		}

		data_bytes_used +=
		    state.alp_state.bp_size +
		    (state.alp_state.exceptions_count * (sizeof(EXACT_TYPE) + AlpConstants::EXCEPTION_POSITION_SIZE)) +
		    AlpConstants::EXPONENT_SIZE +
		    AlpConstants::FACTOR_SIZE +
		    AlpConstants::EXCEPTIONS_COUNT_SIZE +
		    AlpConstants::FOR_SIZE +
		    AlpConstants::BW_SIZE;

		// Write pointer to the vector data (metadata)
		metadata_ptr -= sizeof(uint32_t);
		Store<uint32_t>(next_vector_byte_index_start, metadata_ptr);
		next_vector_byte_index_start = UsedSpace();

		vectors_flushed++;
		vector_idx = 0;
		nulls_idx = 0;
		ResetVector();
	}

	void FlushSegment(){
		auto &checkpoint_state = checkpointer.GetCheckpointState();
		auto dataptr = handle.Ptr();

		idx_t metadata_offset = AlignValue(UsedSpace());

		// Verify that the metadata_ptr is not smaller than the space used by the data
		D_ASSERT(dataptr + metadata_offset <= metadata_ptr);

		idx_t bytes_used_by_metadata = dataptr + Storage::BLOCK_SIZE - metadata_ptr;

		// Initially the total segment size is the size of the block
		idx_t total_segment_size = Storage::BLOCK_SIZE;

		//! We compact the block if the space used is less than a threshold
		const auto used_space_percentage =
		    static_cast<float>(metadata_offset + bytes_used_by_metadata) / static_cast<float>(total_segment_size);
		if (used_space_percentage < AlpConstants::COMPACT_BLOCK_THRESHOLD){
#ifdef DEBUG
			//! Copy the first 4 bytes of the metadata
			uint32_t verify_bytes;
			memcpy((void *)&verify_bytes, metadata_ptr, 4);
#endif
			memmove(dataptr + metadata_offset, metadata_ptr, bytes_used_by_metadata);
#ifdef DEBUG
			//! Now assert that the memmove was correct
			D_ASSERT(verify_bytes == *(uint32_t *)(dataptr + metadata_offset));
#endif
			total_segment_size = metadata_offset + bytes_used_by_metadata;
		}

		// Store the offset to the end of metadata (to be used as a backwards pointer in decoding)
		Store<uint32_t>(total_segment_size, dataptr);

		handle.Destroy();
		checkpoint_state.FlushSegment(std::move(current_segment), total_segment_size);
		data_bytes_used = 0;
		vectors_flushed = 0;
	}

	void Finalize() {
		if (vector_idx != 0){
			CompressVector();
		}
		FlushSegment();
		current_segment.reset();
	}


	void Append(UnifiedVectorFormat &vdata, idx_t count){
		auto data = UnifiedVectorFormat::GetData<T>(vdata);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			T value = data[idx];
			bool is_null = !vdata.validity.RowIsValid(idx);
			//! We resolve null values with a predicated comparison
			vector_null_positions[nulls_idx] = vector_idx;
			nulls_idx += is_null;
			input_vector[vector_idx] = value;
			vector_idx++;
			if (vector_idx == AlpConstants::ALP_VECTOR_SIZE){
				CompressVector();
			}
		}
	}


};

template <class T>
unique_ptr<CompressionState> AlpInitCompression(ColumnDataCheckpointer &checkpointer,
                                                          unique_ptr<AnalyzeState> state) {
	return make_uniq<AlpCompressionState<T>>(checkpointer, (AlpAnalyzeState<T> *)state.get());
}

template <class T>
void AlpCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (AlpCompressionState<T> &)state_p;
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	state.Append(vdata, count);
}

template <class T>
void AlpFinalizeCompress(CompressionState &state_p) {
	auto &state = (AlpCompressionState<T> &)state_p;
	state.Finalize();
}

} // namespace duckdb