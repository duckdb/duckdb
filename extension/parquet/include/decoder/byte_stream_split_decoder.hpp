//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decoder/byte_stream_split_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_bss_decoder.hpp"

namespace duckdb {
class ColumnReader;

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
};

} // namespace duckdb
