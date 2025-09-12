//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/read_duckdb.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct ReadDuckDBTableFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb
