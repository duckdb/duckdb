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

#include <stdio.h>

namespace duckdb {

struct EmptyChimpWriter;

template <class T>
struct ChimpAnalyzeState : public AnalyzeState {
public:
	ChimpAnalyzeState() : state((void *)this) {
		state.chimp_state.SetOutputBuffer(nullptr);
	}
	ChimpState<T, true> state;
	idx_t group_idx = 0;
	idx_t written_bits = 0;
	//! Keep track of when a segment would end, to accurately simulate Reset()s in compress step
	idx_t written_bits_in_current_segment = 0;

public:
	void WriteValue(uint64_t value, bool is_valid) {
		if (!is_valid) {
			return;
		}
		if (!HasEnoughSpace()) {
			StartNewSegment();
		}
		duckdb_chimp::Chimp128Compression<true>::Store(value, state.chimp_state);
		group_idx++;
		if (group_idx == ChimpPrimitives::CHIMP_SEQUENCE_SIZE) {
			StartNewGroup();
		}
	}

	void StartNewSegment() {
		state.template Flush<EmptyChimpWriter>();
		StartNewGroup();
		written_bits += written_bits_in_current_segment;
		written_bits_in_current_segment = 0;
	}

	void StartNewGroup() {
		group_idx = 0;
		written_bits_in_current_segment += state.chimp_state.output.BitsWritten();
		state.chimp_state.Reset();
	}

	idx_t UsedSpace() const {
		return written_bits_in_current_segment + state.chimp_state.output.BitsWritten();
	}

	bool HasEnoughSpace() {
		return UsedSpace() + ChimpPrimitives::MAX_BITS_PER_VALUE <= Storage::BLOCK_SIZE * 8;
	}

	idx_t TotalUsedBytes() const {
		return (written_bits / 8) + (written_bits % 8 != 0);
	}
};

struct EmptyChimpWriter {
	template <class VALUE_TYPE>
	static void Operation(VALUE_TYPE uncompressed_value, bool is_valid, void *state_p) {
		auto state_wrapper = (ChimpAnalyzeState<VALUE_TYPE> *)state_p;
		state_wrapper->WriteValue(*(typename ChimpType<VALUE_TYPE>::type *)(&uncompressed_value), is_valid);
	}
};

template <class T>
unique_ptr<AnalyzeState> ChimpInitAnalyze(ColumnData &col_data, PhysicalType type) {
	return make_unique<ChimpAnalyzeState<T>>();
}

template <class T>
bool ChimpAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	auto &analyze_state = (ChimpAnalyzeState<T> &)state;
	UnifiedVectorFormat vdata;
	input.ToUnifiedFormat(count, vdata);

	auto data = (T *)vdata.data;
	for (idx_t i = 0; i < count; i++) {
		auto idx = vdata.sel->get_index(i);
		analyze_state.state.template Update<EmptyChimpWriter>(data[idx], vdata.validity.RowIsValid(idx));
	}
	return true;
}

template <class T>
idx_t ChimpFinalAnalyze(AnalyzeState &state) {
	auto &chimp_state = (ChimpAnalyzeState<T> &)state;
	// Finish the last "segment"
	chimp_state.StartNewSegment();
	return chimp_state.TotalUsedBytes();
}

} // namespace duckdb
