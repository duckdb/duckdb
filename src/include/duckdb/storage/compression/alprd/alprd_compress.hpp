//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/alp/alp_compress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/alprd/alprd.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/alprd/alprd_analyze.hpp"

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
struct AlpRDCompressionState : public CompressionState {

public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;
	explicit AlpRDCompressionState(ColumnDataCheckpointer &checkpointer, AlpRDAnalyzeState<T> *analyze_state)
	    : checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_ALPRD)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = (void *)this;
		input_vector = vector<EXACT_TYPE>(AlpRDConstants::ALP_VECTOR_SIZE, 0);
		raw_input_vector = vector<T>(AlpRDConstants::ALP_VECTOR_SIZE, 0);
		vector_null_positions = vector<uint16_t>(AlpRDConstants::ALP_VECTOR_SIZE, 0);

		//! State variables from the analyze step that are needed for compression
		memcpy((void*) state.alp_state.left_parts_dict,
		       (void*) analyze_state->state.alp_state.left_parts_dict, AlpRDConstants::DICTIONARY_SIZE_BYTES);
		state.alp_state.left_parts_dict_map = analyze_state->state.alp_state.left_parts_dict_map;
		state.alp_state.left_bw = analyze_state->state.alp_state.left_bw;
		state.alp_state.right_bw = analyze_state->state.alp_state.right_bw;
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
	uint32_t next_vector_byte_index_start = AlpRDConstants::HEADER_SIZE + AlpRDConstants::DICTIONARY_SIZE_BYTES;

	vector<EXACT_TYPE> input_vector;
	vector<T> raw_input_vector;
	vector<uint16_t> vector_null_positions;

	AlpRDState<T, false> state;

public:

	// Returns the space currently used in the segment (in bytes)
	idx_t UsedSpace() const {
		//! [Pointer to metadata + right bitwidth] + Dictionary Size + Bytes already used in the segment
		return AlpRDConstants::HEADER_SIZE + AlpRDConstants::DICTIONARY_SIZE_BYTES + data_bytes_used;
	}

	// Returns the required space to store the newly compressed vector
	idx_t RequiredSpace() {
		idx_t required_space =
		    state.alp_state.left_bp_size +
		    state.alp_state.right_bp_size +
		    state.alp_state.exceptions_count *
		        (AlpRDConstants::EXCEPTION_SIZE + AlpRDConstants::EXCEPTION_POSITION_SIZE) +
		    AlpRDConstants::EXCEPTIONS_COUNT_SIZE;
		return required_space;
	}


	bool HasEnoughSpace() {
		//! If [start of block + used space + required space] is more than whats left (current position
		//! of metadata pointer - the size of a new metadata pointer)
		if ((handle.Ptr() + AlignValue(UsedSpace() + RequiredSpace())) >=
		    (metadata_ptr - AlpRDConstants::METADATA_POINTER_SIZE)){
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
		compressed_segment->function = function;
		current_segment = std::move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		// Pointer to the start of the compressed data
		data_ptr = 
		    handle.Ptr() + current_segment->GetBlockOffset() + 
		    AlpRDConstants::HEADER_SIZE + AlpRDConstants::DICTIONARY_SIZE_BYTES;
		// Pointer to the start of the Metadata
		metadata_ptr = handle.Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE;

		next_vector_byte_index_start = AlpRDConstants::HEADER_SIZE + AlpRDConstants::DICTIONARY_SIZE_BYTES;

	}

	void ReplaceNullsInVector(){
		// Finding the first non-null value
		idx_t tmp_null_idx = 0;
		T a_non_null_raw_value = 0;
		EXACT_TYPE a_non_null_value = 0;
		for (idx_t i = 0; i < vector_idx; i++){
			if (i != vector_null_positions[tmp_null_idx]){
				a_non_null_value = input_vector[i];
				a_non_null_raw_value = raw_input_vector[i];
				break;
			}
			tmp_null_idx += 1;
		}
		// Replacing that first non-null value on the vector
		for (idx_t i = 0; i < nulls_idx; i++){
			uint16_t null_value_pos = vector_null_positions[i];
			input_vector[null_value_pos] = a_non_null_value;
			raw_input_vector[null_value_pos] = a_non_null_raw_value;
		}
	}

	void CompressVector(){
		ReplaceNullsInVector();
		alp::AlpRDCompression<T, false>::Compress(input_vector, vector_idx, state.alp_state);
		//! Check if the compressed vector fits on current segment
		if (!HasEnoughSpace()){
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}
		if (vector_idx != nulls_idx) { //! At least there is one valid value in the vector
			for (idx_t i = 0; i < vector_idx; i++){
				NumericStats::Update<T>(current_segment->stats.statistics, raw_input_vector[i]);
			}
		}
		current_segment->count += vector_idx;
		FlushVector();
	}

	// Stores the vector and its metadata
	void FlushVector(){
		Store<uint16_t>(state.alp_state.exceptions_count ,data_ptr);
		data_ptr += AlpRDConstants::EXCEPTIONS_COUNT_SIZE;

		memcpy((void *) data_ptr, (void*) state.alp_state.left_parts_encoded, state.alp_state.left_bp_size);
		data_ptr += state.alp_state.left_bp_size;

		memcpy((void *) data_ptr, (void*) state.alp_state.right_parts_encoded, state.alp_state.right_bp_size);
		data_ptr += state.alp_state.right_bp_size;

		if (state.alp_state.exceptions_count > 0){
			memcpy((void *) data_ptr, (void*) state.alp_state.exceptions,
			       AlpRDConstants::EXCEPTION_SIZE * state.alp_state.exceptions_count);
			data_ptr += AlpRDConstants::EXCEPTION_SIZE * state.alp_state.exceptions_count;
			memcpy((void *) data_ptr, (void*) state.alp_state.exceptions_positions,
			       AlpRDConstants::EXCEPTION_POSITION_SIZE * state.alp_state.exceptions_count);
			data_ptr += AlpRDConstants::EXCEPTION_POSITION_SIZE * state.alp_state.exceptions_count;
		}

		data_bytes_used +=
		    state.alp_state.left_bp_size +
		    state.alp_state.right_bp_size +
		    (state.alp_state.exceptions_count *
		     (AlpRDConstants::EXCEPTION_SIZE + AlpRDConstants::EXCEPTION_POSITION_SIZE)) +
		    AlpRDConstants::EXCEPTIONS_COUNT_SIZE;

		// Write pointer to the vector data (metadata)
		metadata_ptr -= AlpRDConstants::METADATA_POINTER_SIZE;
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
		dataptr += AlpRDConstants::METADATA_POINTER_SIZE;

		// Store the right bw for the segment
		Store<uint8_t>(state.alp_state.right_bw, dataptr);
		dataptr += AlpRDConstants::R_BW_SIZE;

		// Store the Dictionary
		memcpy((void *) dataptr, (void*) state.alp_state.left_parts_dict, AlpRDConstants::DICTIONARY_SIZE_BYTES);

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
			EXACT_TYPE value = Load<EXACT_TYPE>(const_data_ptr_cast(&data[idx]));
			//! We resolve null values with a predicated comparison
			bool is_null = !vdata.validity.RowIsValid(idx);
			vector_null_positions[nulls_idx] = vector_idx;
			nulls_idx += is_null;
			//! For convenience purposes we store here a parallel vector with the raw type instead of the EXACT_TYPE
			input_vector[vector_idx] = value;
			raw_input_vector[vector_idx] = data[idx];
			vector_idx++;
			if (vector_idx == AlpConstants::ALP_VECTOR_SIZE){
				CompressVector();
			}

		}
	}

};

template <class T>
unique_ptr<CompressionState> AlpRDInitCompression(ColumnDataCheckpointer &checkpointer,
                                                          unique_ptr<AnalyzeState> state) {
	return make_uniq<AlpRDCompressionState<T>>(checkpointer, (AlpRDAnalyzeState<T> *)state.get());
}

template <class T>
void AlpRDCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (AlpRDCompressionState<T> &)state_p;
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	state.Append(vdata, count);
}

template <class T>
void AlpRDFinalizeCompress(CompressionState &state_p) {
	auto &state = (AlpRDCompressionState<T> &)state_p;
	state.Finalize();
}

} // namespace duckdb