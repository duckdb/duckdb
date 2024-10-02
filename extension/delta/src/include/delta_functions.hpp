//===----------------------------------------------------------------------===//
//                         DuckDB
//
// delta_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

class DeltaFunctions {
public:
	static vector<TableFunctionSet> GetTableFunctions(DatabaseInstance &instance);

private:
	static TableFunctionSet GetDeltaScanFunction(DatabaseInstance &instance);
};
} // namespace duckdb
