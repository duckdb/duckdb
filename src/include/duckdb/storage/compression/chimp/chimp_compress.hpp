//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/chimp/chimp_compress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/compression_function.hpp"
#include "duckdb/common/types/null_value.hpp"
#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include <functional>

namespace duckdb {

template <class T>
struct ChimpCompressionState : public CompressionState {};

// Compression Functions

template <class T>
unique_ptr<CompressionState> ChimpInitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                  unique_ptr<AnalyzeState> state) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
	return nullptr;
}

template <class T>
void ChimpCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
}

template <class T>
void ChimpFinalizeCompress(CompressionState &state_p) {
	throw InternalException("Chimp has been deprecated, can no longer be used to compress data");
}

} // namespace duckdb
