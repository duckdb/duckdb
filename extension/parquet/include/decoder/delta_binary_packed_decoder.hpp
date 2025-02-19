//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decoder/delta_binary_packed_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_dbp_decoder.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {
class ColumnReader;

class DeltaBinaryPackedDecoder {
public:
	explicit DeltaBinaryPackedDecoder(ColumnReader &reader);

public:
	void InitializePage();
	void Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);
	void Skip(uint8_t *defines, idx_t skip_count);

private:
	ColumnReader &reader;
	ResizeableBuffer &decoded_data_buffer;
	unique_ptr<DbpDecoder> dbp_decoder;
};

} // namespace duckdb
