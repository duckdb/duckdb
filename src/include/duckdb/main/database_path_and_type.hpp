//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/database_path_and_type.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include "duckdb/main/config.hpp"

namespace duckdb {

struct DBPathAndType {
	//! Parse database extension type and rest of path from combined form (type:path)
	static void ExtractExtensionPrefix(string &path, string &db_type);

	static void CheckMagicBytes(string &path, string &db_type, const DBConfig &config);
};
} // namespace duckdb
