//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/compression/patas/patas_compress.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/compression_function.hpp"

#include "duckdb/common/limits.hpp"

namespace duckdb {

// State

template <class T>
struct PatasCompressionState : public CompressionState {};

// Compression Functions

template <class T>
unique_ptr<CompressionState> PatasInitCompression(ColumnDataCheckpointData &checkpoint_data,
                                                  unique_ptr<AnalyzeState> state) {
	throw InternalException("Patas has been deprecated, can no longer be used to compress data");
	return nullptr;
}

template <class T>
void PatasCompress(CompressionState &state_p, Vector &scan_vector, idx_t count) {
	throw InternalException("Patas has been deprecated, can no longer be used to compress data");
}

template <class T>
void PatasFinalizeCompress(CompressionState &state_p) {
	throw InternalException("Patas has been deprecated, can no longer be used to compress data");
}

} // namespace duckdb
