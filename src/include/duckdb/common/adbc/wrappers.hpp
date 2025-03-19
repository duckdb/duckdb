//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/adbc/wrappers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include <string.h>

namespace duckdb {

struct DuckDBAdbcConnectionWrapper {
	duckdb_connection connection;
	unordered_map<std::string, std::string> options;
};
} // namespace duckdb
