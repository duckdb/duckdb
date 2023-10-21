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
		null_positions = vector<uint16_t>(AlpConstants::ALP_VECTOR_SIZE, 0);

		// Combinations from the analyze step are needed for compression
		state.alp_state.combinations = analyze_state->state.alp_state.combinations;

	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;
	idx_t group_idx = 0;
	idx_t null_idx = 0;
	idx_t groups_flushed = 0;
	idx_t data_bytes_used = 0;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;
	data_ptr_t metadata_ptr;
	uint32_t next_group_byte_index_start = AlpConstants::HEADER_SIZE;

	vector<T> input_vector;
	vector<uint16_t> null_positions;
	T a_non_null_value = 0;

	AlpState<T, false> state;

public:

	// Space currently used in the segment
	idx_t UsedSpace() const {
		// header of segment + bytes used in the segment
		return AlpConstants::METADATA_POINTER_SIZE + data_bytes_used;
	}

	// Required space to store the newly compressed group
	idx_t RequiredSpace() {
		idx_t required_space =
		    state.alp_state.bp_size + // FOR ENCODED
		    (state.alp_state.exceptions_count * (sizeof(EXACT_TYPE) + AlpConstants::EXCEPTION_POSITION_SIZE)) + // Exceptions
		    AlpConstants::EXPONENT_SIZE + // E
		    AlpConstants::FACTOR_SIZE +  // F
		    AlpConstants::EXCEPTIONS_COUNT_SIZE + // Exceptions Count
		    AlpConstants::FOR_SIZE + // FOR
		    AlpConstants::BW_SIZE;  // BW
		    //! Pointer to next group not needed because HasEnoughSpace already take it into account
		return required_space;
	}


	bool HasEnoughSpace() {
		// If start of block + used space + required space is more than whats left (current position
		// of metadata pointer - the size of the new metadata pointer to data)
		if ((handle.Ptr() + AlignValue(UsedSpace() + RequiredSpace())) >= (metadata_ptr - AlpConstants::METADATA_POINTER_SIZE)){
			return false;
		}
		return true;
	}

	void ResetGroup(){
		a_non_null_value = 0;
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

		// Start of Data
		data_ptr = handle.Ptr() + current_segment->GetBlockOffset() + AlpConstants::HEADER_SIZE;
		// Start of Metadata
		metadata_ptr = handle.Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE;

		next_group_byte_index_start = AlpConstants::HEADER_SIZE;

		// state.alp_state.Reset(); // This is wrong, because empty segments are created after compression happened

	}

	// Replace NULL values with the first Non-NULL value on the vector
	void CleanGroup() {
		// Finding the first non-null value
		idx_t tmp_null_idx = 0;
		for (idx_t i = 0; i < group_idx; i++){
			if (i != null_positions[tmp_null_idx]){
				a_non_null_value = input_vector[i];
				break;
			}
			tmp_null_idx += 1;
		}
		// Replacing it on the vector
		for (idx_t j = 0; j < null_idx; j++){
			uint16_t null_value_pos = null_positions[j];
			input_vector[null_value_pos] = a_non_null_value;
		}
	}

	void CompressGroup(){
		CleanGroup();
		alp::AlpCompression<T, false>::Compress(input_vector, group_idx, state.alp_state);
		// Check if group fits on current segment
		if (!HasEnoughSpace()){
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}

		if (group_idx != null_idx){ // At least there is one valid value
			for (idx_t i = 0; i < group_idx; i++){
				NumericStats::Update<T>(current_segment->stats.statistics, input_vector[i]);
			}
		}
		current_segment->count += group_idx;
		FlushGroup();
	}

	void FlushGroup(){
		// Write Data
		//printf("Flushing Group Exponent %d\n", state.alp_state.v_exponent);
		//printf("Flushing Group Exponent %d\n", state.alp_state.v_factor);
		Store<uint8_t>(state.alp_state.v_exponent ,data_ptr);
		data_ptr += AlpConstants::EXPONENT_SIZE;
		Store<uint8_t>(state.alp_state.v_factor ,data_ptr);
		data_ptr += AlpConstants::FACTOR_SIZE;
		Store<uint16_t>(state.alp_state.exceptions_count ,data_ptr);
		data_ptr += AlpConstants::EXCEPTIONS_COUNT_SIZE;
		Store<uint64_t>(state.alp_state.frame_of_reference ,data_ptr);
		data_ptr += AlpConstants::FOR_SIZE;
		Store<uint8_t>(state.alp_state.bit_width ,data_ptr);
		data_ptr += AlpConstants::BW_SIZE;
		memcpy((void *) data_ptr, (void*) state.alp_state.encoded, state.alp_state.bp_size);
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
		    state.alp_state.bp_size + // FOR ENCODED
		    (state.alp_state.exceptions_count * (sizeof(EXACT_TYPE) + AlpConstants::EXCEPTION_POSITION_SIZE)) + // Exceptions
		    AlpConstants::EXPONENT_SIZE + // E
		    AlpConstants::FACTOR_SIZE +  // F
		    AlpConstants::EXCEPTIONS_COUNT_SIZE + // Exceptions Count
		    AlpConstants::FOR_SIZE + // FOR
		    AlpConstants::BW_SIZE; // BW

		// Write MetaData
		metadata_ptr -= sizeof(uint32_t);
		Store<uint32_t>(next_group_byte_index_start, metadata_ptr);
		next_group_byte_index_start = UsedSpace();
		//printf("NEXT GROUP BYTE START!!!! %d\n", next_group_byte_index_start);

		groups_flushed++;
		group_idx = 0;
		null_idx = 0;
		ResetGroup();
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

		// Compact the block only if the space used is less than 80%
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
		// Store the offset to the end of metadata (to be used as a backwards pointer)
		Store<uint32_t>(total_segment_size, dataptr);
		handle.Destroy();
		checkpoint_state.FlushSegment(std::move(current_segment), total_segment_size);
		//printf("Total bytes used %ld\n", data_bytes_used);
		//printf("Total segment size %ld\n", total_segment_size);
		//printf("Block size %d\n", Storage::BLOCK_SIZE);
		//printf("Groups flushed %ld\n", groups_flushed);
		data_bytes_used = 0;
		groups_flushed = 0;
	}

	void Finalize() {
		if (group_idx != 0){
			CompressGroup();
		}
		//printf("\n\nFINALIZE; FLUSHING SEGMENT \n");
		FlushSegment();
		//printf("Block size %d\n", Storage::BLOCK_SIZE);
		current_segment.reset();
		//printf("=============================== FINISH COMPRESSION\n");
	}


	void Append(UnifiedVectorFormat &vdata, idx_t count){
		auto data = UnifiedVectorFormat::GetData<T>(vdata);
		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			T value = data[idx];
			bool is_null = !vdata.validity.RowIsValid(idx);
			null_positions[null_idx] = group_idx;
			null_idx += is_null;
			input_vector[group_idx] = value;
			group_idx++;
			if (group_idx == AlpConstants::ALP_VECTOR_SIZE){
				//printf("Group Starting... \n");
				CompressGroup();
				//printf("Group Finished... \n");
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