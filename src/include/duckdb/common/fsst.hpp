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

namespace duckdb {
class Value;
class Vector;
struct string_t;

class FSSTPrimitives {
public:
	static string_t DecompressValue(void *duckdb_fsst_decoder, Vector &result, const char *compressed_string,
	                                const idx_t compressed_string_len, vector<unsigned char> &decompress_buffer);
	static string DecompressValue(void *duckdb_fsst_decoder, const char *compressed_string,
	                              const idx_t compressed_string_len, vector<unsigned char> &decompress_buffer);
};
} // namespace duckdb
