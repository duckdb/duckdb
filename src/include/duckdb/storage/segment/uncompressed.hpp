//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/segment/uncompressed.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/segment/base_segment.hpp"

namespace duckdb {
class DatabaseInstance;

struct UncompressedFunctions {
	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer, unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState& state_p, Vector &data, idx_t count);
	static void FinalizeCompress(CompressionState& state_p);
};

struct NumericUncompressed {
	static CompressionFunction GetFunction(PhysicalType data_type);
};

struct ValidityUncompressed {
	static const validity_t LOWER_MASKS[65];
	static const validity_t UPPER_MASKS[65];

	static CompressionFunction GetFunction(PhysicalType data_type);
};

} // namespace duckdb
