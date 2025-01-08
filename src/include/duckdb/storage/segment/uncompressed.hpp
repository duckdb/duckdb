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
	static unique_ptr<CompressionState> InitCompression(ColumnDataCheckpointData &checkpoint_data,
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
	static void AlignedScan(data_ptr_t input, idx_t input_start, Vector &result, idx_t scan_count);
	static void UnalignedScan(data_ptr_t input, idx_t input_size, idx_t input_start, Vector &result,
	                          idx_t result_offset, idx_t scan_count);

public:
	static const validity_t LOWER_MASKS[65];
	static const validity_t UPPER_MASKS[65];
};

struct StringUncompressed {
public:
	static CompressionFunction GetFunction(PhysicalType data_type);
	static idx_t GetStringBlockLimit(const idx_t block_size) {
		return MinValue(AlignValueFloor(block_size / 4), DEFAULT_STRING_BLOCK_LIMIT);
	}

public:
	//! The default maximum string size for sufficiently big block sizes
	static constexpr idx_t DEFAULT_STRING_BLOCK_LIMIT = 4096;
};

} // namespace duckdb
