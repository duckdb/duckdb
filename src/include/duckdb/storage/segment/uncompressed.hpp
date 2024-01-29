//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/segment/uncompressed.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/column_segment.hpp"

namespace duckdb {
class DatabaseInstance;

struct UncompressedFunctions {
	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointer &checkpointer,
	                                                    unique_ptr<AnalyzeState> state);
	static void Compress(CompressionState &state_p, Vector &data, idx_t count);
	static void FinalizeCompress(CompressionState &state_p);
	static void EmptySkip(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count) {
	}
};

struct FixedSizeUncompressed {
	static CompressionFunction GetFunction(PhysicalType data_type);
};

struct ValidityUncompressed {
public:
	static CompressionFunction GetFunction(PhysicalType data_type);

public:
	static const validity_t LOWER_MASKS[65];
	static const validity_t UPPER_MASKS[65];
};

struct StringUncompressed {
public:
	static CompressionFunction GetFunction(PhysicalType data_type);

public:
	//! The default maximum string size for sufficiently big block sizes
	static constexpr uint16_t DEFAULT_STRING_BLOCK_LIMIT = 4096;
	//! The maximum string size within a block. We offload bigger strings to overflow blocks
	static constexpr uint16_t STRING_BLOCK_LIMIT =
	    MinValue(AlignValueFloor(Storage::BLOCK_SIZE / 4), idx_t(DEFAULT_STRING_BLOCK_LIMIT));
};

//! Detect mismatching constant values
static_assert(StringUncompressed::STRING_BLOCK_LIMIT != 0, "the string block limit cannot be 0");

} // namespace duckdb
