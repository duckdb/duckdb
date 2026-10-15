//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decoder/byte_stream_split_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>

#include "duckdb.hpp"
#include "parquet_bss_decoder.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/shared_ptr.hpp"

namespace duckdb {
class ColumnReader;
class ResizeableBuffer;
class Vector;

class ByteStreamSplitDecoder {
public:
	explicit ByteStreamSplitDecoder(ColumnReader &reader);

public:
	void InitializePage();
	void Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);
	void Skip(uint8_t *defines, idx_t skip_count);

private:
	ColumnReader &reader;
	ResizeableBuffer &decoded_data_buffer;
	unique_ptr<BssDecoder> bss_decoder;
	//! FIXED_LEN_BYTE_ARRAY pages are decoded up front into PLAIN layout. The buffer is shared so that
	//! string readers can keep it alive for the strings that point into it (as for DELTA_BYTE_ARRAY).
	shared_ptr<ResizeableBuffer> plain_data;
};

} // namespace duckdb
