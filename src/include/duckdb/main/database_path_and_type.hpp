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
	static DBPathAndType Parse(const string &combined_path, const DBConfig &config);

	const string path;
	const string type;
};
} // namespace duckdb
