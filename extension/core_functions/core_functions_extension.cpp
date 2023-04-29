#define DUCKDB_EXTENSION_MAIN
#include "core_functions_extension.hpp"
#include "function_list.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

template<class T>
void FillExtraInfo(StaticFunctionDefinition &function, T &info) {
	info.internal = true;
	info.description = function.description;
	info.parameter_names = StringUtil::Split(function.parameters, ",");
	info.example = function.example;
}

void CoreFunctionsExtension::RegisterFunctions(Catalog &catalog, CatalogTransaction transaction) {
	auto functions = StaticFunctionDefinition::GetFunctionList();
	for (idx_t i = 0; functions[i].name; i++) {
		auto &function = functions[i];
		if (function.get_function || function.get_function_set) {
			// scalar function
			ScalarFunctionSet result;
			if (function.get_function) {
				result.AddFunction(function.get_function());
			} else {
				result = function.get_function_set();
			}
			result.name = function.name;
			CreateScalarFunctionInfo info(result);
			FillExtraInfo(function, info);
			catalog.CreateFunction(transaction, info);
		} else if (function.get_aggregate_function || function.get_aggregate_function_set) {
			// aggregate function
			AggregateFunctionSet result;
			if (function.get_aggregate_function) {
				result.AddFunction(function.get_aggregate_function());
			} else {
				result = function.get_aggregate_function_set();
			}
			result.name = function.name;
			CreateAggregateFunctionInfo info(result);
			FillExtraInfo(function, info);
			catalog.CreateFunction(transaction, info);
		} else {
			throw InternalException("Do not know how to register function of this type");
		}
	}
}

void CoreFunctionsExtension::Load(DuckDB &ddb) {
	auto &db = *ddb.instance;
	auto &catalog = Catalog::GetSystemCatalog(db);
	CatalogTransaction transaction(db, 1, 1);
	RegisterFunctions(catalog, transaction);
}

std::string CoreFunctionsExtension::Name() {
	return "core_functions";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void core_functions_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::CoreFunctionsExtension>();
}

DUCKDB_EXTENSION_API const char *core_functions_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
