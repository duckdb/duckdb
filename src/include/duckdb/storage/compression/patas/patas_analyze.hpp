//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/patas/patas_analyze.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/compression/patas/patas.hpp"
#include "duckdb/function/compression_function.hpp"

namespace duckdb {

struct EmptyPatasWriter;

template <class T>
struct PatasAnalyzeState : public AnalyzeState {};

template <class T>
unique_ptr<AnalyzeState> PatasInitAnalyze(ColumnData &col_data, PhysicalType type) {
	// This compression type is deprecated
	return nullptr;
}

template <class T>
bool PatasAnalyze(AnalyzeState &state, Vector &input, idx_t count) {
	throw InternalException("Patas has been deprecated, can no longer be used to compress data");
	return false;
}

template <class T>
idx_t PatasFinalAnalyze(AnalyzeState &state) {
	throw InternalException("Patas has been deprecated, can no longer be used to compress data");
}

} // namespace duckdb
