#include "delta_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

namespace duckdb {

vector<TableFunctionSet> DeltaFunctions::GetTableFunctions(DatabaseInstance &instance) {
	vector<TableFunctionSet> functions;

	// Main delta_scan function
	functions.push_back(GetDeltaScanFunction(instance));
	
	// V2 metadata inspection functions
	functions.push_back(GetDeltaTableInfoFunction(instance));
	functions.push_back(GetDeltaFileStatsFunction(instance));

	return functions;
}

}; // namespace duckdb
