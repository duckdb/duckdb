//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decoder/delta_byte_array_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_dbp_decoder.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {
class ColumnReader;

class DeltaByteArrayDecoder {
public:
	explicit DeltaByteArrayDecoder(ColumnReader &reader);

public:
	void InitializePage();

	void Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);
	void Skip(uint8_t *defines, idx_t skip_count);

	static void ReadDbpData(Allocator &allocator, ResizeableBuffer &buffer, ResizeableBuffer &result_buffer,
	                        idx_t &value_count);

private:
	ColumnReader &reader;

	//! Decoded data in plain Parquet page format
	shared_ptr<ResizeableBuffer> plain_data;
};

} // namespace duckdb
