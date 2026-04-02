//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/adbc/wrappers.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.h"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb_adbc {
struct DuckDBAdbcStreamWrapper;
} // namespace duckdb_adbc

namespace duckdb {

struct DuckDBAdbcConnectionWrapper {
	duckdb_connection connection;
	unordered_map<string, string> options;
	//! Active stream wrappers on this connection (for materialization on concurrent execute)
	vector<duckdb_adbc::DuckDBAdbcStreamWrapper *> active_streams;
};
} // namespace duckdb
