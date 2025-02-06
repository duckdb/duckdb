//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decoder/rle_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_rle_bp_decoder.hpp"

namespace duckdb {
class ColumnReader;

class RLEDecoder {
public:
	explicit RLEDecoder(ColumnReader &reader);

public:
	void InitializePage();
	void Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);

private:
	ColumnReader &reader;
	unique_ptr<RleBpDecoder> rle_decoder;
};

} // namespace duckdb
