//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/chimp_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/chimp/chimp.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

struct EmptyChimpWriter;

template <class T>
struct ChimpAnalyzeState : public AnalyzeState {};

template <class T>
unique_ptr<AnalyzeState> ChimpInitAnalyze(ColumnData &col_data, PhysicalType type) {
	// This compression type is deprecated
	return nullptr;
}

template <class T>
bool ChimpAnalyze(AnalyzeState &state, const Vector &input) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
	return false;
}

template <class T>
idx_t ChimpFinalAnalyze(AnalyzeState &state) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
}

} // namespace duckdb
