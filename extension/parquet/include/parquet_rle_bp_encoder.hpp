//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_rle_bp_encoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parquet_types.h"
#include "thrift_tools.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {

class RleBpEncoder {
public:
	explicit RleBpEncoder(uint32_t bit_width);

public:
	//! NOTE: Prepare is only required if a byte count is required BEFORE writing
	//! This is the case with e.g. writing repetition/definition levels
	//! If GetByteCount() is not required, prepare can be safely skipped
	void BeginPrepare(uint32_t first_value);
	void PrepareValue(uint32_t value);
	void FinishPrepare();

	void BeginWrite(WriteStream &writer, uint32_t first_value);
	void WriteValue(WriteStream &writer, uint32_t value);
	void FinishWrite(WriteStream &writer);

	idx_t GetByteCount();

private:
	//! meta information
	uint32_t byte_width;
	//! RLE run information
	idx_t byte_count;
	idx_t run_count;
	idx_t current_run_count;
	uint32_t last_value;

private:
	void FinishRun();
	void WriteRun(WriteStream &writer);
};

} // namespace duckdb
