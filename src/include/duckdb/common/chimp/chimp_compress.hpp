//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/bitpacking.hpp
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

#include <stdio.h>

#include <functional>

namespace duckdb {

// State

template <class T>
struct ChimpCompressionState : public CompressionState {
public:
	struct ChimpWriter {

		template <class VALUE_TYPE>
		static void Operation(VALUE_TYPE *values, bool *validity, idx_t count, void *state_p) {
			//! Need access to the CompressionState to be able to flush the segment
			auto state_wrapper = (ChimpCompressionState<VALUE_TYPE> *)state_p;

			//! We might need to check this based on how much is already written
			//! We could check this for every 128 values, but then we would need to cut our sequence down to 128,
			//! because our space calculation for 1024 values would not be correct anymore Realistically this should
			//! never happen, because these values should always fit in a Block, unless CHIMP_SEQUENCE_SIZE gets changed
			D_ASSERT(state_wrapper->HasEnoughSpace());

			for (idx_t i = 0; i < count; i++) {
				if (validity[i]) {
					NumericStatistics::Update<VALUE_TYPE>(state_wrapper->current_segment->stats, values[i]);
				}
			}

			if (count) {
				state_wrapper->WriteValues((uint64_t *)values, count);
			}
			if (count == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
				auto row_start = state_wrapper->current_segment->start + state_wrapper->current_segment->count;
				state_wrapper->FlushSegment();
				state_wrapper->CreateEmptySegment(row_start);
			}
		}
	};

	explicit ChimpCompressionState(ColumnDataCheckpointer &checkpointer, ChimpAnalyzeState<T> *analyze_state)
	    : bytes_needed_to_compress(analyze_state->state.chimp_state.CompressedSize()), checkpointer(checkpointer) {

		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto &config = DBConfig::GetConfig(db);
		function = config.GetCompressionFunction(CompressionType::COMPRESSION_CHIMP, type.InternalType());
		CreateEmptySegment(checkpointer.GetRowGroup().start);

		state.data_ptr = (void *)this;
	}

	//! Computed in the Analyze step
	idx_t bytes_needed_to_compress;

	ColumnDataCheckpointer &checkpointer;
	CompressionFunction *function;
	unique_ptr<ColumnSegment> current_segment;
	BufferHandle handle;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;

	ChimpState<T, false> state;

public:
	idx_t RequiredSpace() {
		return bytes_needed_to_compress;
	}

	bool HasEnoughSpace() {
		return RequiredSpace() <= Storage::BLOCK_SIZE;
	}

	void CreateEmptySegment(idx_t row_start) {
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		data_ptr = handle.Ptr() + current_segment->GetBlockOffset();
		state.chimp_state.SetOutputBuffer(data_ptr);
	}

	void Append(UnifiedVectorFormat &vdata, idx_t count) {
		auto data = (T *)vdata.data;

		for (idx_t i = 0; i < count; i++) {
			auto idx = vdata.sel->get_index(i);
			state.template Update<ChimpWriter>(data, vdata.validity, idx);
		}
	}

	void WriteValues(uint64_t *values, idx_t count) {
		D_ASSERT(count);

		printf("COMPRESS\n");
		printf("--- %f ---\n", values[0]);
		duckdb_chimp::Chimp128Compression<false>::template Store<true>(values[0], state.chimp_state);
		for (idx_t i = 1; i < count; i++) {
			printf("--- %f ---\n", values[i]);
			duckdb_chimp::Chimp128Compression<false>::template Store<false>(values[i], state.chimp_state);
		}
		auto bits_written = state.chimp_state.output->BitsWritten();
		printf("writtenBits: %llu\n", bits_written);
		auto bytes_written = (bits_written / 8) + (bits_written % 8 != 0);
		auto stream = (uint8_t *)state.chimp_state.output->Stream();
		for (idx_t i = 0; i < bytes_written; i++) {
			auto temp_string = toBinaryString<uint8_t>(stream[i]);
			printf("%s\n", temp_string.c_str());
		}
		duckdb_chimp::Chimp128Compression<false>::Close(state.chimp_state);
		current_segment->count += count;
	}

	void FlushSegment() {
		auto &checkpoint_state = checkpointer.GetCheckpointState();
		handle.Destroy();

		auto bits_written = state.chimp_state.CompressedSize();
		auto total_segment_size = bits_written / 8 + (bits_written % 8 != 0);
		checkpoint_state.FlushSegment(move(current_segment), total_segment_size);
	}

	void Finalize() {
		state.template Flush<ChimpWriter>();
		FlushSegment();
		current_segment.reset();
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
