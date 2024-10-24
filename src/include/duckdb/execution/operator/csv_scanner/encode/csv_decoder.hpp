//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/csv_scanner/encode/csv_decoder.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/execution/operator/csv_scanner/encode/csv_encoding.hpp"
#include "duckdb/common/file_system.hpp"

namespace duckdb {

class CSVDecoder {
public:
	explicit CSVDecoder(CSVEncoding encoding);
	bool IsUTF8() const;
	idx_t Decode(FileHandle &file_handle, void *buffer, const idx_t nr_bytes) const;

private:
	CSVEncoding encoding;
	idx_t GetRatio() const;
	void DecodeInternal(const char *encoded_buffer, idx_t encoded_buffer_size, char *decoded_buffer,
	                    idx_t &decoded_buffer_start) const;

	//! Actual decoding functions
	static void DecodeUTF16(const char *encoded_buffer, idx_t encoded_buffer_size, char *decoded_buffer,
	                        idx_t &decoded_buffer_start);
	static void DecodeLatin1(const char *encoded_buffer, idx_t encoded_buffer_size, char *decoded_buffer,
	                         idx_t &decoded_buffer_start);
};
} // namespace duckdb
