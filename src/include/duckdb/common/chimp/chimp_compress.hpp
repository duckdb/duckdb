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
	idx_t written_bits = 0;
	idx_t group_idx = 0;

	// Ptr to next free spot in segment;
	data_ptr_t data_ptr;

	ChimpState<T, false> state;

public:
	idx_t RequiredSpace() const {
		return ChimpPrimitives::MAX_BITS_PER_VALUE;
	}
	idx_t UsedSpace() const {
		return state.chimp_state.output.BitsWritten();
	}

	bool HasEnoughSpace() {
		return UsedSpace() + RequiredSpace() <= (Storage::BLOCK_SIZE * 8);
	}

	void CreateEmptySegment(idx_t row_start) {
		written_bits = 0;
		auto &db = checkpointer.GetDatabase();
		auto &type = checkpointer.GetType();
		auto compressed_segment = ColumnSegment::CreateTransientSegment(db, type, row_start);
		compressed_segment->function = function;
		current_segment = move(compressed_segment);

		auto &buffer_manager = BufferManager::GetBufferManager(db);
		handle = buffer_manager.Pin(current_segment->block);

		data_ptr = handle.Ptr() + current_segment->GetBlockOffset();
		state.chimp_state.SetOutputBuffer(data_ptr);
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
			group_idx = 0;
			written_bits += UsedSpace();
			state.chimp_state.Reset();
		}
	}

	void FlushSegment() {
		state.chimp_state.output.Flush();
		auto &checkpoint_state = checkpointer.GetCheckpointState();
		handle.Destroy();

		idx_t total_bits_written = written_bits + UsedSpace();
		idx_t total_segment_size = total_bits_written + (total_bits_written % 8 != 0);
		checkpoint_state.FlushSegment(move(current_segment), total_segment_size);
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
