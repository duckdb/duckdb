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
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class DatabaseInstance;
class ColumnData;
class ColumnDataCheckpointer;
class ColumnSegment;
class SegmentStatistics;

struct ColumnFetchState;
struct ColumnScanState;
struct SegmentScanState;

struct AnalyzeState {
	virtual ~AnalyzeState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct CompressionState {
	virtual ~CompressionState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct CompressedSegmentState {
	virtual ~CompressedSegmentState() {
	}

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

struct CompressionAppendState {
	CompressionAppendState(BufferHandle handle_p) : handle(std::move(handle_p)) {
	}
	virtual ~CompressionAppendState() {
	}

	BufferHandle handle;

	template <class TARGET>
	TARGET &Cast() {
		D_ASSERT(dynamic_cast<TARGET *>(this));
		return reinterpret_cast<TARGET &>(*this);
	}
	template <class TARGET>
	const TARGET &Cast() const {
		D_ASSERT(dynamic_cast<const TARGET *>(this));
		return reinterpret_cast<const TARGET &>(*this);
	}
};

//===--------------------------------------------------------------------===//
// Analyze
//===--------------------------------------------------------------------===//
//! The analyze functions are used to determine whether or not to use this compression method
//! The system first determines the potential compression methods to use based on the physical type of the column
//! After that the following steps are taken:
//! 1. The init_analyze is called to initialize the analyze state of every candidate compression method
//! 2. The analyze method is called with all of the input data in the order in which it must be stored.
//!    analyze can return "false". In that case, the compression method is taken out of consideration early.
//! 3. The final_analyze method is called, which should return a score for the compression method

//! The system then decides which compression function to use based on the analyzed score (returned from final_analyze)
typedef unique_ptr<AnalyzeState> (*compression_init_analyze_t)(ColumnData &col_data, PhysicalType type);
typedef bool (*compression_analyze_t)(AnalyzeState &state, Vector &input, idx_t count);
typedef idx_t (*compression_final_analyze_t)(AnalyzeState &state);

//===--------------------------------------------------------------------===//
// Compress
//===--------------------------------------------------------------------===//
typedef unique_ptr<CompressionState> (*compression_init_compression_t)(ColumnDataCheckpointer &checkpointer,
                                                                       unique_ptr<AnalyzeState> state);
typedef void (*compression_compress_data_t)(CompressionState &state, Vector &scan_vector, idx_t count);
typedef void (*compression_compress_finalize_t)(CompressionState &state);

//===--------------------------------------------------------------------===//
// Uncompress / Scan
//===--------------------------------------------------------------------===//
typedef unique_ptr<SegmentScanState> (*compression_init_segment_scan_t)(ColumnSegment &segment);

//! Function prototype used for reading an entire vector (STANDARD_VECTOR_SIZE)
typedef void (*compression_scan_vector_t)(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                          Vector &result);
//! Function prototype used for reading an arbitrary ('scan_count') number of values
typedef void (*compression_scan_partial_t)(ColumnSegment &segment, ColumnScanState &state, idx_t scan_count,
                                           Vector &result, idx_t result_offset);
//! Function prototype used for reading a single value
typedef void (*compression_fetch_row_t)(ColumnSegment &segment, ColumnFetchState &state, row_t row_id, Vector &result,
                                        idx_t result_idx);
//! Function prototype used for skipping 'skip_count' values, non-trivial if random-access is not supported for the
//! compressed data.
typedef void (*compression_skip_t)(ColumnSegment &segment, ColumnScanState &state, idx_t skip_count);

//===--------------------------------------------------------------------===//
// Append (optional)
//===--------------------------------------------------------------------===//
typedef unique_ptr<CompressedSegmentState> (*compression_init_segment_t)(ColumnSegment &segment, block_id_t block_id);
typedef unique_ptr<CompressionAppendState> (*compression_init_append_t)(ColumnSegment &segment);
typedef idx_t (*compression_append_t)(CompressionAppendState &append_state, ColumnSegment &segment,
                                      SegmentStatistics &stats, UnifiedVectorFormat &data, idx_t offset, idx_t count);
typedef idx_t (*compression_finalize_append_t)(ColumnSegment &segment, SegmentStatistics &stats);
typedef void (*compression_revert_append_t)(ColumnSegment &segment, idx_t start_row);

class CompressionFunction {
public:
	CompressionFunction(CompressionType type, PhysicalType data_type, compression_init_analyze_t init_analyze,
	                    compression_analyze_t analyze, compression_final_analyze_t final_analyze,
	                    compression_init_compression_t init_compression, compression_compress_data_t compress,
	                    compression_compress_finalize_t compress_finalize, compression_init_segment_scan_t init_scan,
	                    compression_scan_vector_t scan_vector, compression_scan_partial_t scan_partial,
	                    compression_fetch_row_t fetch_row, compression_skip_t skip,
	                    compression_init_segment_t init_segment = nullptr,
	                    compression_init_append_t init_append = nullptr, compression_append_t append = nullptr,
	                    compression_finalize_append_t finalize_append = nullptr,
	                    compression_revert_append_t revert_append = nullptr)
	    : type(type), data_type(data_type), init_analyze(init_analyze), analyze(analyze), final_analyze(final_analyze),
	      init_compression(init_compression), compress(compress), compress_finalize(compress_finalize),
	      init_scan(init_scan), scan_vector(scan_vector), scan_partial(scan_partial), fetch_row(fetch_row), skip(skip),
	      init_segment(init_segment), init_append(init_append), append(append), finalize_append(finalize_append),
	      revert_append(revert_append) {
	}

	//! Compression type
	CompressionType type;
	//! The data type this function can compress
	PhysicalType data_type;

	//! Analyze step: determine which compression function is the most effective
	//! init_analyze is called once to set up the analyze state
	compression_init_analyze_t init_analyze;
	//! analyze is called several times (once per vector in the row group)
	//! analyze should return true, unless compression is no longer possible with this compression method
	//! in that case false should be returned
	compression_analyze_t analyze;
	//! final_analyze should return the score of the compression function
	//! ideally this is the exact number of bytes required to store the data
	//! this is not required/enforced: it can be an estimate as well
	//! also this function can return DConstants::INVALID_INDEX to skip this compression method
	compression_final_analyze_t final_analyze;

	//! Compression step: actually compress the data
	//! init_compression is called once to set up the comperssion state
	compression_init_compression_t init_compression;
	//! compress is called several times (once per vector in the row group)
	compression_compress_data_t compress;
	//! compress_finalize is called after
	compression_compress_finalize_t compress_finalize;

	//! init_scan is called to set up the scan state
	compression_init_segment_scan_t init_scan;
	//! scan_vector scans an entire vector using the scan state
	compression_scan_vector_t scan_vector;
	//! scan_partial scans a subset of a vector
	//! this can request > vector_size as well
	//! this is used if a vector crosses segment boundaries, or for child columns of lists
	compression_scan_partial_t scan_partial;
	//! fetch an individual row from the compressed vector
	//! used for index lookups
	compression_fetch_row_t fetch_row;
	//! Skip forward in the compressed segment
	compression_skip_t skip;

	// Append functions
	//! This only really needs to be defined for uncompressed segments

	//! Initialize a compressed segment (optional)
	compression_init_segment_t init_segment;
	//! Initialize the append state (optional)
	compression_init_append_t init_append;
	//! Append to the compressed segment (optional)
	compression_append_t append;
	//! Finalize an append to the segment
	compression_finalize_append_t finalize_append;
	//! Revert append (optional)
	compression_revert_append_t revert_append;
};

//! The set of compression functions
struct CompressionFunctionSet {
	mutex lock;
	map<CompressionType, map<PhysicalType, CompressionFunction>> functions;
};

} // namespace duckdb
