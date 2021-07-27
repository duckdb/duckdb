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
class ColumnData;

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
// skip function
// fetch function

//! The type used for initializing hashed aggregate function states
class CompressionFunction {
public:
	CompressionFunction(CompressionType type, PhysicalType data_type, compression_init_analyze_t init_analyze,
	                    compression_analyze_t analyze, compression_final_analyze_t final_analyze,
	                    compression_init_compression_t init_compression, compression_compress_data_t compress,
	                    compression_flush_state_t flush) :
	type(type), data_type(data_type), init_analyze(init_analyze), analyze(analyze), final_analyze(final_analyze),
	init_compression(init_compression), compress(compress), flush(flush) {}

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
};

//! The set of compression functions
struct CompressionFunctionSet {
	map<CompressionType, map<PhysicalType, CompressionFunction>> functions;
};

} // namespace duckdb
