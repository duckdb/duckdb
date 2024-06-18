//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/fsst.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/typedefs.hpp"

namespace duckdb {
class Value;
class Vector;
struct string_t;

class FSSTPrimitives {
public:
	static string_t DecompressValue(void *duckdb_fsst_decoder, Vector &result, const char *compressed_string,
	                                const idx_t compressed_string_len, const idx_t block_size);
	static Value DecompressValue(void *duckdb_fsst_decoder, const char *compressed_string,
	                             const idx_t compressed_string_len, const idx_t block_size);
};
} // namespace duckdb
