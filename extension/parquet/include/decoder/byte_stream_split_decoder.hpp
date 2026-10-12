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

namespace duckdb {
class ColumnReader;
class ResizeableBuffer;
class Vector;

class ByteStreamSplitDecoder {
public:
	explicit ByteStreamSplitDecoder(ColumnReader &reader);

public:
	void InitializePage();
	void Rebase();
	void Read(uint8_t *defines, idx_t read_count, Vector &result, idx_t result_offset);
	void Skip(uint8_t *defines, idx_t skip_count);

private:
	ColumnReader &reader;
	ResizeableBuffer &decoded_data_buffer;
	unique_ptr<BssDecoder> bss_decoder;
	//! Fixed offset (from block's allocation base) where bss_decoder's region begins, captured once at
	//! InitializePage().
	idx_t region_offset = 0;
};

} // namespace duckdb
