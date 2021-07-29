//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/compression_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/common/map.hpp"

namespace duckdb {
class DatabaseInstance;
class ColumnData;
class CompressedSegment;
class SegmentStatistics;

struct ColumnFetchState;
struct ColumnScanState;
struct SegmentScanState;

struct AnalyzeState {
	virtual ~AnalyzeState(){}
};

struct CompressionState {
	virtual ~CompressionState(){}
};

//! The analyze functions are used to determine whether or not to use this compression method
//! The system first determines the potential compression methods to use based on the physical type of the column
//! After that the following steps are taken:
//! 1. The init_analyze is called to initialize the analyze state of every candidate compression method
//! 2. The analyze method is called with all of the input data in the order in which it must be stored.
//!    analyze can return "false". In that case, the compression method is taken out of consideration early.
//! 3. The final_analyze method is called, which should return a score for the compression method

//! The system then decides based on:
//! (1) the analyzed score, and
//! (2) the read_speed and write_speed of the compression
//! which compression method to use (if any)
typedef unique_ptr<AnalyzeState> (*compression_init_analyze_t)(ColumnData &col_data, PhysicalType type);
typedef bool (*compression_analyze_t)(AnalyzeState &state, Vector &input, idx_t count);
typedef idx_t (*compression_final_analyze_t)(AnalyzeState &state);

typedef unique_ptr<CompressionState> (*compression_init_compression_t)(AnalyzeState &state);
// FIXME: this should not take a vector, but this should be different function pointers for flat/constant/vectordata orrify
typedef idx_t (*compression_compress_data_t)(BufferHandle &block, idx_t data_written, Vector& intermediate, idx_t count, CompressionState& state);
typedef idx_t (*compression_flush_state_t)(BufferHandle &block, idx_t data_written, CompressionState& state);

// init scan function state
// scan function
typedef unique_ptr<SegmentScanState> (*compression_init_segment_scan_t)(CompressedSegment &segment);
typedef void (*compression_scan_vector_t)(CompressedSegment &segment, ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result);
typedef void (*compression_scan_partial_t)(CompressedSegment &segment, ColumnScanState &state, idx_t start, idx_t scan_count, Vector &result, idx_t result_offset);
typedef void (*compression_fetch_row_t)(CompressedSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result, idx_t result_idx);
typedef idx_t (*compression_append_t)(CompressedSegment &segment, SegmentStatistics &stats, VectorData &data, idx_t offset, idx_t count);

//! The type used for initializing hashed aggregate function states
class CompressionFunction {
public:
	CompressionFunction(CompressionType type, PhysicalType data_type, compression_init_analyze_t init_analyze,
	                    compression_analyze_t analyze, compression_final_analyze_t final_analyze,
	                    compression_init_compression_t init_compression, compression_compress_data_t compress,
	                    compression_flush_state_t flush, compression_init_segment_scan_t init_scan,
	                    compression_scan_vector_t scan_vector, compression_scan_partial_t scan_partial,
	                    compression_fetch_row_t fetch_row, compression_append_t append) :
	type(type), data_type(data_type), init_analyze(init_analyze), analyze(analyze), final_analyze(final_analyze),
	init_compression(init_compression), compress(compress), flush(flush), init_scan(init_scan),
	scan_vector(scan_vector), scan_partial(scan_partial), fetch_row(fetch_row), append(append) {}

	//! Compression type
	CompressionType type;
	//! The data type this function can compress
	PhysicalType data_type;

	compression_init_analyze_t init_analyze;
	compression_analyze_t analyze;
	compression_final_analyze_t final_analyze;
	compression_init_compression_t init_compression;
	compression_compress_data_t compress;
	compression_flush_state_t flush;
	compression_init_segment_scan_t init_scan;
	compression_scan_vector_t scan_vector;
	compression_scan_partial_t scan_partial;
	compression_fetch_row_t fetch_row;
	compression_append_t append;
};

//! The set of compression functions
struct CompressionFunctionSet {
	map<CompressionType, map<PhysicalType, CompressionFunction>> functions;
};

} // namespace duckdb
