//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/chimp/chimp_compress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/chimp/chimp.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/chimp/chimp_analyze.hpp"

#include "duckdb/common/limits.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/function/compression/compression.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include <functional>

namespace duckdb {

// State

template <class T>
struct ChimpCompressionState : public CompressionState {
public:
	struct ChimpWriter {

		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE value, bool is_valid, void *state_p) {
			//! Need access to the CompressionState to be able to flush the segment
			auto state_wrapper = (ChimpCompressionState<VALUE_TYPE> *)state_p;

			if (is_valid && !state_wrapper->HasEnoughSpace()) {
				// Segment is full
				auto row_start = state_wrapper->current_segment->start + state_wrapper->current_segment->count;
				state_wrapper->FlushSegment();
				state_wrapper->CreateEmptySegment(row_start);
			}

			if (is_valid) {
				NumericStatistics::Update<VALUE_TYPE>(state_wrapper->current_segment->stats, value);
			}

			state_wrapper->WriteValue(*(typename ChimpType<VALUE_TYPE>::type *)(&value), is_valid);
		}
	};

	explicit ChimpCompressionState(ColumnDataCheckpointer &checkpointer, ChimpAnalyzeState<T> *analyze_state)
	    : checkpointer(checkpointer) {

		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_CHIMP, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = (void *)this;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;
	idx_t group_idx = 0;
	uint8_t flags[ChimpPrimitives::CHIMP_SEQUENCE_SIZE / 4];

	// Ptr to next free spot in segment;
	data_ptr_t segment_data;
	data_ptr_t metadata_ptr;
	uint32_t metadata_bit_offset;
	uint32_t next_group_bit_index_start = 0;

	ChimpState<T, false> state;

public:
	idx_t RequiredSpace() const {
		idx_t required_space = ChimpPrimitives::MAX_BITS_PER_VALUE;
		if (group_idx == 0) {
			// New group, will require a new 'bit_index' offset to be recorded
			required_space += (sizeof(bit_index_t) * 8);
		}
		return required_space;
	}

	idx_t UsedSpace() const {
		return state.chimp_state.output.BitsWritten();
	}

	idx_t MetadataSpaceUsed() const {
		idx_t metadata_size = metadata_bit_offset + ChimpPrimitives::FLAG_BIT_SIZE; // new value has a flag
		bool byte_aligned = metadata_size % 8 == 0;
		if (byte_aligned) {
			return metadata_size;
		}
		return metadata_size + 8 - (metadata_size % 8);
	}

	bool HasEnoughSpace() {
		return UsedSpace() + RequiredSpace() + MetadataSpaceUsed() <= Storage::BLOCK_SIZE * 8;
	}

	void CreateEmptySegment(idx_t row_start) {
		group_idx = 0;
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		segment_data = handle.Ptr() + current_segment->GetBlockOffset() + ChimpPrimitives::HEADER_SIZE;
		metadata_ptr = handle.Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE;
		state.AssignBuffers(segment_data);
		state.chimp_state.Reset();
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = (T *)vdata.data;

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<ChimpWriter>(data[idx], vdata.validity.RowIsValid(idx));
		}
	}

	void WriteValue(uint64_t value, bool is_valid) {
		current_segment->count++;
		if (!is_valid) {
			return;
		}
		duckdb_chimp::Chimp128Compression<false>::Store(value, state.chimp_state);
		group_idx++;
		metadata_bit_offset += ChimpPrimitives::FLAG_BIT_SIZE;
		if (group_idx == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
			FlushGroup();
		}
	}

	void FlushGroup() {
		metadata_ptr -= sizeof(bit_index_t);
		Store<bit_index_t>(metadata_ptr, next_group_bit_index_start);
		next_group_bit_index_start = UsedSpace();
		metadata_ptr -= (AlignValue(group_idx * ChimpPrimitives::FLAG_BIT_SIZE) / 8);
		memcpy(metadata_ptr, flags, group_idx / 4);
		state.chimp_state.Reset();
	}

	void FlushSegment() {
		FlushGroup();
		state.chimp_state.output.Flush();
		auto &checkpoint_state = checkpointer.GetCheckpointState();
		auto dataptr = handle.Ptr();

		// Compact the segment by moving the metadata next to the data.
		idx_t bytes_used_by_data = AlignValue(UsedSpace()) / 8;
		idx_t metadata_offset = AlignValue(bytes_used_by_data);
		idx_t metadata_size = dataptr + Storage::BLOCK_SIZE - metadata_ptr - 1;
		idx_t total_segment_size = metadata_offset + metadata_size;
		memmove(dataptr + metadata_offset, metadata_ptr + 1, metadata_size);

		// Store the offset of the metadata of the first group (which is at the highest address).
		Store<idx_t>(metadata_offset + metadata_size - 1, dataptr);
		handle.Destroy();

		checkpoint_state.FlushSegment(move(current_segment), total_segment_size);
	}

	void Finalize() {
		FlushSegment();
		current_segment.reset();
	}

private:
	// Space remaining between the metadata_ptr growing down and data ptr growing up
	idx_t RemainingSize() {
		const auto distance = metadata_ptr - segment_data;
		const idx_t bits_written = state.chimp_state.output.BitsWritten();
		return distance - ((bits_written / 8) + (bits_written % 8 != 0));
	}
};

// Compression Functions

template <class T>
unique_ptr<CompressionState> ChimpInitCompression(ColumnDataCheckpointer &checkpointer,
                                                  unique_ptr<AnalyzeState> state) {
	return make_unique<ChimpCompressionState<T>>(checkpointer, (ChimpAnalyzeState<T> *)state.get());
}

template <class T>
void ChimpCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (ChimpCompressionState<T> &)state_p;
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	state.Append(vdata, count);
}

template <class T>
void ChimpFinalizeCompress(CompressionState &state_p) {
	auto &state = (ChimpCompressionState<T> &)state_p;
	state.Finalize();
}

} // namespace duckdb
