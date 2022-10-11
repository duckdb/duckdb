//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/patas/patas_compress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/bitpacking.hpp"
#include "duckdb/storage/compression/patas/patas.hpp"
#include "duckdb/function/compression_function.hpp"
#include "duckdb/storage/compression/patas/patas_analyze.hpp"

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
struct PatasCompressionState : public CompressionState {
public:
	using EXACT_TYPE = typename FloatingToExact<T>::type;

	struct PatasWriter {

		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE value, bool is_valid, void *state_p) {
			//! Need access to the CompressionState to be able to flush the segment
			auto state_wrapper = (PatasCompressionState<VALUE_TYPE> *)state_p;

			if (!state_wrapper->HasEnoughSpace()) {
				// Segment is full
				auto row_start = state_wrapper->current_segment->start + state_wrapper->current_segment->count;
				state_wrapper->FlushSegment();
				state_wrapper->CreateEmptySegment(row_start);
			}

			if (is_valid) {
				NumericStatistics::Update<VALUE_TYPE>(state_wrapper->current_segment->stats, value);
			}

			state_wrapper->WriteValue(*(EXACT_TYPE *)(&value));
		}
	};

	explicit PatasCompressionState(ColumnDataCheckpointer &checkpointer, PatasAnalyzeState<T> *analyze_state)
	    : checkpointer(checkpointer) {

		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_PATAS, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = (void *)this;
		state.patas_state.Reset();
	}

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;
	idx_t group_idx = 0;

	// Ptr to next free spot in segment;
	data_ptr_t segment_data;
	data_ptr_t metadata_ptr;
	uint32_t next_group_byte_index_start = PatasPrimitives::HEADER_SIZE;
	// The total size of metadata in the current segment
	idx_t metadata_byte_size = 0;

	PatasState<T, false> state;

public:
	idx_t RequiredSpace() const {
		idx_t required_space = sizeof(EXACT_TYPE);
		// byte offset of data
		required_space += sizeof(byte_index_t);
		// amount of bitpacking blocks
		required_space += sizeof(uint8_t);
		// first bitpacked 'trailing_zero' block
		required_space += ((6 * 32) / 8); // 24
		// first bitpacked 'byte_count' block
		required_space += ((3 * 32) / 8); // 12
		return required_space;
	}

	// How many bytes the data occupies for the current segment
	idx_t UsedSpace() const {
		return state.patas_state.byte_writer.BytesWritten();
	}

	idx_t RemainingSpace() const {
		return metadata_ptr - (handle.Ptr() + UsedSpace());
	}

	idx_t CurrentGroupMetadataSize() const {
		idx_t metadata_size = 0;

		const idx_t effective_bitpack_block_index = AlignValue<idx_t, 32>(state.patas_state.index);

		// Current bytes taken up by the bitpacked 'trailing_zero' blocks
		metadata_size += (effective_bitpack_block_index * PatasPrimitives::TRAILING_ZERO_BITSIZE) / 8;

		// Current bytes taken up by the bitpacked 'byte_count' blocks
		metadata_size += (effective_bitpack_block_index * PatasPrimitives::BYTECOUNT_BITSIZE) / 8;
		return metadata_size;
	}

	// The current segment has enough space to fit this new value
	bool HasEnoughSpace() {
		if (handle.Ptr() + AlignValue(PatasPrimitives::HEADER_SIZE + UsedSpace() + RequiredSpace()) >=
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

		segment_data = handle.Ptr() + PatasPrimitives::HEADER_SIZE;
		metadata_ptr = handle.Ptr() + Storage::BLOCK_SIZE;
		state.AssignDataBuffer(segment_data);
		state.patas_state.Reset();
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = (T *)vdata.data;

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<PatasWriter>(data[idx], vdata.validity.RowIsValid(idx));
		}
	}

	void WriteValue(EXACT_TYPE value) {
		current_segment->count++;
		patas::PatasCompression<EXACT_TYPE, false>::Store(value, state.patas_state);
		group_idx++;
		if (group_idx == PatasPrimitives::PATAS_GROUP_SIZE) {
			FlushGroup();
		}
	}

	template <uint8_t BITWIDTH>
	void FlushBitpackedData(uint8_t *src, uint8_t *dest, uint8_t block_count) {
		BitpackingPrimitives::PackBuffer<uint8_t>(
		    dest, src, block_count * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE, BITWIDTH);
	}

	void FlushGroup() {
		metadata_ptr -= sizeof(byte_index_t);
		metadata_byte_size += sizeof(byte_index_t);
		// Store where this groups data starts, relative to the start of the segment
		Store<byte_index_t>(next_group_byte_index_start, metadata_ptr);
		next_group_byte_index_start = UsedSpace();

		uint8_t bitpacked_block_count =
		    AlignValue<idx_t, BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE>(state.patas_state.index) /
		    BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE;
		// FIXME: is this necessary, cant we just derive this from the count of the segment??
		D_ASSERT(bitpacked_block_count <=
		         (PatasPrimitives::PATAS_GROUP_SIZE / BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE));
		metadata_ptr -= sizeof(uint8_t);
		metadata_byte_size += sizeof(uint8_t);
		// Store how many bitpacked blocks there are (storing BITPACKING_ALGORITHM_GROUP_SIZE values per block)
		Store<uint8_t>(bitpacked_block_count, metadata_ptr);

		const uint64_t trailing_zeros_bits =
		    (PatasPrimitives::TRAILING_ZERO_BITSIZE * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) *
		    bitpacked_block_count;
		const uint64_t byte_counts_bits =
		    (PatasPrimitives::BYTECOUNT_BITSIZE * BitpackingPrimitives::BITPACKING_ALGORITHM_GROUP_SIZE) *
		    bitpacked_block_count;
		metadata_ptr -= AlignValue(trailing_zeros_bits) / 8;
		metadata_byte_size += AlignValue(trailing_zeros_bits) / 8;
		// Bitpack + store the 'trailing_zero' values
		FlushBitpackedData<PatasPrimitives::TRAILING_ZERO_BITSIZE>(state.patas_state.trailing_zeros, metadata_ptr,
		                                                           bitpacked_block_count);

		metadata_ptr -= AlignValue(byte_counts_bits) / 8;
		metadata_byte_size += AlignValue(byte_counts_bits) / 8;
		// Bitpack + store the 'byte_count' values
		FlushBitpackedData<PatasPrimitives::BYTECOUNT_BITSIZE>(state.patas_state.byte_counts, metadata_ptr,
		                                                       bitpacked_block_count);

		state.patas_state.Reset();
		group_idx = 0;
	}

	//! FIXME: only compact if the unused space meets a certain threshold (20%)
	void FlushSegment() {
		//! TODO: only flush the group if the group idx is not 0
		if (group_idx != 0) {
			FlushGroup();
		}
		auto &checkpoint_state = checkpointer.GetCheckpointState();
		auto dataptr = handle.Ptr();

		// Compact the segment by moving the metadata next to the data.
		idx_t bytes_used_by_data = PatasPrimitives::HEADER_SIZE + UsedSpace();
		idx_t metadata_offset = AlignValue(bytes_used_by_data);
		// Verify that the metadata_ptr does not cross this threshold
		D_ASSERT(dataptr + metadata_offset < metadata_ptr);
		idx_t metadata_size = dataptr + Storage::BLOCK_SIZE - metadata_ptr;
		idx_t total_segment_size = metadata_offset + metadata_size;
#ifdef DEBUG
		//! Copy the first 4 bytes of the metadata
		uint32_t verify_bytes;
		std::memcpy((void *)&verify_bytes, metadata_ptr, 4);
#endif
		memmove(dataptr + metadata_offset, metadata_ptr, metadata_size);
#ifdef DEBUG
		//! Now assert that the memmove was correct
		D_ASSERT(verify_bytes == *(uint32_t *)(dataptr + metadata_offset));
#endif
		// Store the offset to the metadata
		Store<uint32_t>(metadata_offset + metadata_size, dataptr);
		handle.Destroy();
		checkpoint_state.FlushSegment(move(current_segment), total_segment_size);
		// printf("COMPRESS: DATA BYTES SIZE: %llu\n", UsedSpace());
	}

	void Finalize() {
		FlushSegment();
		current_segment.reset();
	}
};

// Compression Functions

template <class T>
unique_ptr<CompressionState> PatasInitCompression(ColumnDataCheckpointer &checkpointer,
                                                  unique_ptr<AnalyzeState> state) {
	return make_unique<PatasCompressionState<T>>(checkpointer, (PatasAnalyzeState<T> *)state.get());
}

template <class T>
void PatasCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	auto &state = (PatasCompressionState<T> &)state_p;
	UnifiedVectorFormat vdata;
	scan_vector.ToUnifiedFormat(count, vdata);
	state.Append(vdata, count);
}

template <class T>
void PatasFinalizeCompress(CompressionState &state_p) {
	auto &state = (PatasCompressionState<T> &)state_p;
	state.Finalize();
}

} // namespace duckdb
