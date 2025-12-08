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
	//! Main delta_scan table function
	static TableFunctionSet GetDeltaScanFunction(DatabaseInstance &instance);
	
	//! delta_table_info - returns table metadata including V2 features
	static TableFunctionSet GetDeltaTableInfoFunction(DatabaseInstance &instance);
	
	//! delta_file_stats - returns per-file statistics including deletion vectors
	static TableFunctionSet GetDeltaFileStatsFunction(DatabaseInstance &instance);
};
} // namespace duckdb
