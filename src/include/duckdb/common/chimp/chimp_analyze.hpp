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

// template <typename T>
// std::string toBinaryString(const T &x) {
//	std::stringstream ss;
//	ss << std::bitset<sizeof(T) * 8>(x);
//	return ss.str();
// }

template <class T>
struct ChimpAnalyzeState : public AnalyzeState {
public:
	ChimpAnalyzeState() : state((void *)this) {
		state.chimp_state.SetOutputBuffer(nullptr);
	}
	ChimpState<T, true> state;

public:
	void WriteValue(uint64_t value) {
		printf("ANALYZE\n");
		duckdb_chimp::Chimp128Compression<true>::Store(value, state.chimp_state);
	}
};

struct EmptyChimpWriter {
	template <class VALUE_TYPE>
	static void Operation(VALUE_TYPE uncompressed_value, bool is_valid, void *state_p) {
		auto state_wrapper = (ChimpAnalyzeState<VALUE_TYPE> *)state_p;
		state_wrapper->WriteValue(*(typename ChimpType<VALUE_TYPE>::type *)(&uncompressed_value));
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
	analyze_state.state.template Flush<EmptyChimpWriter>();
	return true;
}

template <class T>
idx_t ChimpFinalAnalyze(AnalyzeState &state) {
	auto &chimp_state = (ChimpAnalyzeState<T> &)state;
	chimp_state.state.template Flush<EmptyChimpWriter>();
	return chimp_state.state.chimp_state.CompressedSize();
}

} // namespace duckdb
