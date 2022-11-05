//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/fsst.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

class FSSTPrimitives {
public:
	static string_t DecompressValue(void *duckdb_fsst_decoder, Vector &result, unsigned char *compressed_string,
	                                idx_t compressed_string_len);
	static Value DecompressValue(void *duckdb_fsst_decoder, unsigned char *compressed_string,
	                             idx_t compressed_string_len);
};
} // namespace duckdb
