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

#include "duckdb/storage/table/column_data_checkpointer.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/common/operator/subtract.hpp"

#include <functional>

namespace duckdb {

// State

template <class T>
struct PatasCompressionState : public CompressionState {};

// Compression Functions

template <class T>
unique_ptr<CompressionState> PatasInitCompression(ColumnDataCheckpointer &checkpointer,
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
