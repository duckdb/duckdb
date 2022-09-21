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

template <typename T>
std::string toBinaryString(const T &x) {
	std::stringstream ss;
	ss << std::bitset<sizeof(T) * 8>(x);
	return ss.str();
}

template <class T>
struct ChimpAnalyzeState : public AnalyzeState {
public:
	ChimpAnalyzeState() : state((void *)this) {
		state.chimp_state.SetOutputBuffer(nullptr);
	}
	ChimpState<T, true> state;

public:
	void WriteValues(uint64_t *values, idx_t count) {
		D_ASSERT(count);
		printf("ANALYZE\n");
		printf("--- %f ---\n", values[0]);
		duckdb_chimp::Chimp128Compression<true>::template Store<true>(values[0], state.chimp_state);
		for (idx_t i = 1; i < count; i++) {
			printf("--- %f ---\n", values[i]);
			duckdb_chimp::Chimp128Compression<true>::template Store<false>(values[i], state.chimp_state);
		}
		duckdb_chimp::Chimp128Compression<true>::Flush(state.chimp_state);
		auto bits_written = state.chimp_state.output->BitsWritten();
		printf("writtenBits: %llu\n", bits_written);
	}
};

struct EmptyChimpWriter {
	template <class VALUE_TYPE>
	static void Operation(VALUE_TYPE *values, bool *validity, idx_t count, void *state_p) {
		auto state_wrapper = (ChimpAnalyzeState<VALUE_TYPE> *)state_p;
		if (count) {
			state_wrapper->WriteValues((uint64_t *)values, count);
		}
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
		if (!analyze_state.state.template Update<EmptyChimpWriter>(data, vdata.validity, idx)) {
			return false;
		}
	}
	return true;
}

template <class T>
idx_t ChimpFinalAnalyze(AnalyzeState &state) {
	auto &chimp_state = (ChimpAnalyzeState<T> &)state;
	chimp_state.state.template Flush<EmptyChimpWriter>();
	return chimp_state.state.chimp_state.CompressedSize();
}

} // namespace duckdb
