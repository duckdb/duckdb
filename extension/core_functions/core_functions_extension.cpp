#define DUCKDB_EXTENSION_MAIN
#include "core_functions_extension.hpp"

#include "core_functions/function_list.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

void LoadInternal(DuckDB &db) {
	FunctionList::RegisterExtensionFunctions(*db.instance, CoreFunctionList::GetFunctionList());
}

void CoreFunctionsExtension::Load(DuckDB &db) {
	LoadInternal(db);
}

std::string CoreFunctionsExtension::Name() {
	return "core_functions";
}

std::string CoreFunctionsExtension::Version() const {
#ifdef EXT_VERSION_CORE_FUNCTIONS
	return EXT_VERSION_CORE_FUNCTIONS;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void core_functions_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	duckdb::LoadInternal(db_wrapper);
}

DUCKDB_EXTENSION_API const char *core_functions_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
