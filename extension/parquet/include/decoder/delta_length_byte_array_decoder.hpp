//===----------------------------------------------------------------------===//
//                         DuckDB
//
// decoder/delta_length_byte_array_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "parquet_dbp_decoder.hpp"
#include "resizable_buffer.hpp"

namespace duckdb {
class ColumnReader;

class DeltaLengthByteArrayDecoder {
public:
	explicit DeltaLengthByteArrayDecoder(ColumnReader &reader);

public:
	void InitializePage();

	void Read(shared_ptr<ResizeableBuffer> &block, uint8_t *defines, idx_t read_count, Vector &result,
	          idx_t result_offset);
	void Skip(uint8_t *defines, idx_t skip_count);

private:
	template <bool HAS_DEFINES, bool VALIDATE_INDIVIDUAL_STRINGS>
	void ReadInternal(shared_ptr<ResizeableBuffer> &block, uint8_t *defines, idx_t read_count, Vector &result,
	                  idx_t result_offset);
	template <bool HAS_DEFINES>
	void SkipInternal(uint8_t *defines, idx_t skip_count);

private:
	ColumnReader &reader;
	ResizeableBuffer &length_buffer;
	idx_t byte_array_count = 0;
	idx_t length_idx;
};

} // namespace duckdb
