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

		// These buffers are recycled for every group, so they only have to be set once
		state.AssignLeadingZeroBuffer((uint8_t *)leading_zero_blocks);
		state.AssignFlagBuffer((uint8_t *)flags);

		state.data_ptr = (void *)this;
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;
	idx_t group_idx = 0;
	uint8_t flags[ChimpPrimitives::CHIMP_SEQUENCE_SIZE / 4];
	uint8_t leading_zero_blocks[ChimpPrimitives::LEADING_ZERO_BLOCK_BUFFERSIZE];

	// Ptr to next free spot in segment;
	data_ptr_t segment_data;
	data_ptr_t metadata_ptr;
	uint32_t next_group_byte_index_start = 0;
	// The total size of metadata in the current segment
	idx_t metadata_byte_size = 0;

	ChimpState<T, false> state;

public:
	idx_t RequiredSpace() const {
		idx_t required_space = ChimpPrimitives::MAX_BYTES_PER_VALUE;
		// Any value could be the last,
		// so the cost of flushing metadata should be factored into the cost
		// byte offset of data
		required_space += sizeof(byte_index_t);
		// amount of leading zero blocks
		required_space += sizeof(uint8_t);
		// first leading zero block
		required_space += 3;
		// amount of flag bytes
		required_space += sizeof(uint8_t);
		// first flag byte
		required_space += 1;
		return required_space;
	}

	// How many bytes the data occupies for the current segment
	idx_t UsedSpace() const {
		return state.chimp_state.output.BytesWritten();
	}

	idx_t CurrentGroupMetadataSize() const {
		return (3 * state.chimp_state.leading_zero_buffer.BlockCount()) + state.chimp_state.flag_buffer.BytesUsed();
	}

	// The current segment has enough space to fit this new value
	bool HasEnoughSpace() {
		if (handle.Ptr() + ChimpPrimitives::HEADER_SIZE + AlignValue(UsedSpace() + RequiredSpace()) >=
		    (metadata_ptr - CurrentGroupMetadataSize())) {
			return false;
		}
		return true;
	}

	void CreateEmptySegment(idx_t row_start) {
		group_idx = 0;
		metadata_byte_size = 0;
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		segment_data = handle.Ptr() + current_segment->GetBlockOffset() + ChimpPrimitives::HEADER_SIZE;
		metadata_ptr = handle.Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE;
		state.AssignDataBuffer(segment_data);
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
		if (group_idx == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
			FlushGroup();
		}
	}

	void FlushGroup() {
		// Has to be called first to flush the last values in the LeadingZeroBuffer
		state.chimp_state.Flush();

		metadata_ptr -= sizeof(byte_index_t);
		metadata_byte_size += sizeof(byte_index_t);
		// Store where this groups data starts, relative to the start of the segment
		Store<byte_index_t>(next_group_byte_index_start, metadata_ptr);
		next_group_byte_index_start = UsedSpace();

		const uint8_t leading_zero_block_count = state.chimp_state.leading_zero_buffer.BlockCount();
		metadata_ptr -= sizeof(uint8_t);
		metadata_byte_size += sizeof(uint8_t);
		// Store how many leading zero blocks there are
		Store<uint8_t>(leading_zero_block_count, metadata_ptr);

		const uint64_t bytes_used_by_leading_zero_blocks = 3 * leading_zero_block_count;
		metadata_ptr -= bytes_used_by_leading_zero_blocks;
		metadata_byte_size += bytes_used_by_leading_zero_blocks;
		// Store the leading zeros (8 per 3 bytes) for this group
		memcpy((void *)metadata_ptr, (void *)leading_zero_blocks, bytes_used_by_leading_zero_blocks);

		// FIXME: This is max 256, which BARELY doesn't fit into a single byte
		// instead we could use 0 to denote 256, saving an extra byte every (CHIMP_SEQUENCE_SIZE)1024 values
		const uint16_t flag_bytes = state.chimp_state.flag_buffer.BytesUsed();
		metadata_ptr -= sizeof(uint16_t);
		metadata_byte_size += sizeof(uint16_t);
		// Store how many flag bytes there are
		// We cant use the 'count' of the segment to figure this out, because NULLs increase count
		Store<uint16_t>(flag_bytes, metadata_ptr);

		metadata_ptr -= flag_bytes;
		metadata_byte_size += flag_bytes;
		// Store the flags (4 per byte) for this group
		memcpy((void *)metadata_ptr, (void *)flags, flag_bytes);

		state.chimp_state.Reset();
		group_idx = 0;
	}

	void FlushSegment() {
		FlushGroup();
		state.chimp_state.output.Flush();
		auto &checkpoint_state = checkpointer.GetCheckpointState();
		auto dataptr = handle.Ptr();

		// Compact the segment by moving the metadata next to the data.
		idx_t bytes_used_by_data = ChimpPrimitives::HEADER_SIZE + UsedSpace();
		idx_t metadata_offset = AlignValue(bytes_used_by_data);
		// Verify that the metadata_ptr does not cross this threshold
		D_ASSERT(dataptr + metadata_offset < metadata_ptr);
		idx_t metadata_size = dataptr + Storage::BLOCK_SIZE - metadata_ptr;
		idx_t total_segment_size = metadata_offset + metadata_size;
#ifdef DEBUG
		uint32_t verify_bytes;
		std::memcpy((void *)&verify_bytes, metadata_ptr, 4);
#endif
		memmove(dataptr + metadata_offset, metadata_ptr, metadata_size);
#ifdef DEBUG
		D_ASSERT(verify_bytes == *(uint32_t *)(dataptr + metadata_offset));
#endif
		// Store the offset of the metadata of the first group (which is at the highest address).
		Store<uint32_t>(metadata_offset + metadata_size, dataptr);
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
		const idx_t bits_written = state.chimp_state.output.BytesWritten();
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
