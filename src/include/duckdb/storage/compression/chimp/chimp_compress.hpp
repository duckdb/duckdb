//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/chimp_compress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/chimp.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/chimp/chimp_analyze.hpp"

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
struct ChimpCompressionState : public CompressionState {
public:
	using CHIMP_TYPE = typename ChimpType<T>::type;

	explicit ChimpCompressionState(ColumnDataCheckpointer &checkpointer, ChimpAnalyzeState<T> *analyze_state)
	    : checkpointer(checkpointer),
	      function(checkpointer.GetCompressionFunction(CompressionType::COMPRESSION_CHIMP)) {
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		// These buffers are recycled for every group, so they only have to be set once
		state.AssignLeadingZeroBuffer((uint8_t *)leading_zero_blocks);
		state.AssignFlagBuffer((uint8_t *)flags);
		state.AssignPackedDataBuffer((uint16_t *)packed_data_blocks);
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction &function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;
	idx_t group_idx = 0;
	uint8_t flags[ChimpPrimitives::CHIMP_SEQUENCE_SIZE / 4];
	uint8_t leading_zero_blocks[ChimpPrimitives::LEADING_ZERO_BLOCK_BUFFERSIZE];
	uint16_t packed_data_blocks[ChimpPrimitives::CHIMP_SEQUENCE_SIZE];

	// Ptr to next free spot in segment;
	data_ptr_t segment_data;
	data_ptr_t metadata_ptr;
	uint32_t next_group_byte_index_start = ChimpPrimitives::HEADER_SIZE;
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
		return state.chimp.output.BytesWritten();
	}

	idx_t RemainingSpace() const {
		return metadata_ptr - (handle.Ptr() + UsedSpace());
	}

	idx_t CurrentGroupMetadataSize() const {
		idx_t metadata_size = 0;

		metadata_size += 3 * state.chimp.leading_zero_buffer.BlockCount();
		metadata_size += state.chimp.flag_buffer.BytesUsed();
		metadata_size += 2 * state.chimp.packed_data_buffer.index;
		return metadata_size;
	}

	// The current segment has enough space to fit this new value
	bool HasEnoughSpace() {
		if (handle.Ptr() + AlignValue(ChimpPrimitives::HEADER_SIZE + UsedSpace() + RequiredSpace()) >=
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
		current_segment = std::move(compressed_segment);
		next_group_byte_index_start = ChimpPrimitives::HEADER_SIZE;

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		segment_data = handle.Ptr() + current_segment->GetBlockOffset() + ChimpPrimitives::HEADER_SIZE;
		metadata_ptr = handle.Ptr() + current_segment->GetBlockOffset() + Storage::BLOCK_SIZE;
		state.AssignDataBuffer(segment_data);
		state.chimp.Reset();
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = UnifiedVectorFormat::GetData<CHIMP_TYPE>(vdata);

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			WriteValue(data[idx], vdata.validity.RowIsValid(idx));
		}
	}

	void WriteValue(CHIMP_TYPE value, bool is_valid) {
		if (!HasEnoughSpace()) {
			// Segment is full
			auto row_start = current_segment->start + current_segment->count;
			FlushSegment();
			CreateEmptySegment(row_start);
		}
		current_segment->count++;

		if (is_valid) {
			T floating_point_value = Load<T>(const_data_ptr_cast(&value));
			NumericStats::Update<T>(current_segment->stats.statistics, floating_point_value);
		} else {
			//! FIXME: find a cheaper alternative to storing a NULL
			// store this as "value_identical", only using 9 bits for a NULL
			value = state.chimp.previous_value;
		}

		Chimp128Compression<CHIMP_TYPE, false>::Store(value, state.chimp);
		group_idx++;
		if (group_idx == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
			FlushGroup();
		}
	}

	void FlushGroup() {
		// Has to be called first to flush the last values in the LeadingZeroBuffer
		state.chimp.Flush();

		metadata_ptr -= sizeof(byte_index_t);
		metadata_byte_size += sizeof(byte_index_t);
		// Store where this groups data starts, relative to the start of the segment
		Store<byte_index_t>(next_group_byte_index_start, metadata_ptr);
		next_group_byte_index_start = UsedSpace();

		const uint8_t leading_zero_block_count = state.chimp.leading_zero_buffer.BlockCount();
		// Every 8 values are packed in one block
		D_ASSERT(leading_zero_block_count <= ChimpPrimitives::CHIMP_SEQUENCE_SIZE / 8);
		metadata_ptr -= sizeof(uint8_t);
		metadata_byte_size += sizeof(uint8_t);
		// Store how many leading zero blocks there are
		Store<uint8_t>(leading_zero_block_count, metadata_ptr);

		const uint64_t bytes_used_by_leading_zero_blocks = 3 * leading_zero_block_count;
		metadata_ptr -= bytes_used_by_leading_zero_blocks;
		metadata_byte_size += bytes_used_by_leading_zero_blocks;
		// Store the leading zeros (8 per 3 bytes) for this group
		memcpy((void *)metadata_ptr, (void *)leading_zero_blocks, bytes_used_by_leading_zero_blocks);

		//! This is max 1024, because it's the amount of flags there are, not the amount of bytes that takes up
		const uint16_t flag_bytes = state.chimp.flag_buffer.BytesUsed();
#ifdef DEBUG
		const idx_t padding = (current_segment->count % ChimpPrimitives::CHIMP_SEQUENCE_SIZE) == 0
		                          ? ChimpPrimitives::CHIMP_SEQUENCE_SIZE
		                          : 0;
		const idx_t size_of_group = padding + current_segment->count % ChimpPrimitives::CHIMP_SEQUENCE_SIZE;
		D_ASSERT((AlignValue<idx_t, 4>(size_of_group - 1) / 4) == flag_bytes);
#endif

		metadata_ptr -= flag_bytes;
		metadata_byte_size += flag_bytes;
		// Store the flags (4 per byte) for this group
		memcpy((void *)metadata_ptr, (void *)flags, flag_bytes);

		// Store the packed data blocks (2 bytes each)
		// We dont need to store an extra count for this,
		// as the count can be derived from unpacking the flags and counting the '1' flags

		// FIXME: this does stop us from skipping groups with point queries,
		// because the metadata has a variable size, and we have to extract all flags + iterate them to know this size
		const uint16_t packed_data_blocks_count = state.chimp.packed_data_buffer.index;
		metadata_ptr -= packed_data_blocks_count * 2;
		metadata_byte_size += packed_data_blocks_count * 2;
		if ((uint64_t)metadata_ptr & 1) {
			// Align on a two-byte boundary
			metadata_ptr--;
			metadata_byte_size++;
		}
		memcpy((void *)metadata_ptr, (void *)packed_data_blocks, packed_data_blocks_count * sizeof(uint16_t));

		state.chimp.Reset();
		group_idx = 0;
	}

	// FIXME: only do this if the wasted space meets a certain threshold (>= 20%)
	void FlushSegment() {
		if (group_idx) {
			// Only call this when the group actually has data that needs to be flushed
			FlushGroup();
		}
		state.chimp.output.Flush();
		auto &checkpoint_state = checkpointer.GetCheckpointState();
		auto dataptr = handle.Ptr();

		// Compact the segment by moving the metadata next to the data.
		idx_t bytes_used_by_data = ChimpPrimitives::HEADER_SIZE + UsedSpace();
		idx_t metadata_offset = AlignValue(bytes_used_by_data);
		// Verify that the metadata_ptr does not cross this threshold
		D_ASSERT(dataptr + metadata_offset <= metadata_ptr);
		idx_t metadata_size = dataptr + Storage::BLOCK_SIZE - metadata_ptr;
		idx_t total_segment_size = metadata_offset + metadata_size;
#ifdef DEBUG
		uint32_t verify_bytes;
		memcpy((void *)&verify_bytes, metadata_ptr, 4);
#endif
		memmove(dataptr + metadata_offset, metadata_ptr, metadata_size);
#ifdef DEBUG
		D_ASSERT(verify_bytes == *(uint32_t *)(dataptr + metadata_offset));
#endif
		//  Store the offset of the metadata of the first group (which is at the highest address).
		Store<uint32_t>(metadata_offset + metadata_size, dataptr);
		handle.Destroy();
		checkpoint_state.FlushSegment(std::move(current_segment), total_segment_size);
	}

	void Finalize() {
		FlushSegment();
		current_segment.reset();
	}
};

// Compression Functions

template <class T>
unique_ptr<CompressionState> ChimpInitCompression(ColumnDataCheckpointer &checkpointer,
                                                  unique_ptr<AnalyzeState> state) {
	return make_uniq<ChimpCompressionState<T>>(checkpointer, (ChimpAnalyzeState<T> *)state.get());
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
