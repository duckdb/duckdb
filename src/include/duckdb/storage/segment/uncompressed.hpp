//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/segment/uncompressed.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/compression_function.hpp"

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

	//! ANDs scan_count validity bits from input (starting at input_start) into the result validity mask
	//! (starting at result_offset). If a bit in the result is already invalid (0), it remains invalid
	//! regardless of the input bit value - i.e., the operation is result[i] &= input[i].
	//! This function should be used, as the name suggests, if the starting points are unaligned relative to
	//! ValidityMask::BITS_PER_VALUE, otherwise AlignedScan should be used (however this function will
	//! still work).
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
