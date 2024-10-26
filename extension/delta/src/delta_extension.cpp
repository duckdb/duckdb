#define DUCKDB_EXTENSION_MAIN

#include "delta_extension.hpp"
#include "delta_functions.hpp"

#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &instance) {
	// Load functions
	for (const auto &function : DeltaFunctions::GetTableFunctions(instance)) {
		ExtensionUtil::RegisterFunction(instance, function);
	}
}

void DeltaExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
}

std::string DeltaExtension::Name() {
	return "delta";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void delta_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::DeltaExtension>();
}

DUCKDB_EXTENSION_API const char *delta_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
