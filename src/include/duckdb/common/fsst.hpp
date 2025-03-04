//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/fsst.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/storage/string_uncompressed.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/fsst.hpp"
#include "fsst.h"
#include "duckdb/common/types/vector_buffer.hpp"

namespace duckdb {
class Value;
class Vector;
struct string_t;

class FSSTPrimitives {
public:
	static string_t DecompressValue(void *duckdb_fsst_decoder, Vector &result, const char *compressed_string,
	                                const idx_t compressed_string_len, vector<unsigned char> &decompress_buffer) {
		auto &str_buffer = StringVector::GetStringBuffer(result);
		idx_t max_uncompressed_length = compressed_string_len * 8;
		auto fsst_decoder = reinterpret_cast<duckdb_fsst_decoder_t *>(duckdb_fsst_decoder);
		auto compressed_string_ptr = reinterpret_cast<const unsigned char *>(compressed_string);
		auto target_ptr = str_buffer.AllocateBuffer(max_uncompressed_length);
		auto decompressed_string_size =
		    duckdb_fsst_decompress(fsst_decoder, compressed_string_len, compressed_string_ptr, max_uncompressed_length,
		                           reinterpret_cast<unsigned char *>(target_ptr));
		return str_buffer.FinalizeBuffer(target_ptr, max_uncompressed_length, decompressed_string_size);
	}
	static string DecompressValue(void *duckdb_fsst_decoder, const char *compressed_string,
	                              const idx_t compressed_string_len, vector<unsigned char> &decompress_buffer);
};
} // namespace duckdb
