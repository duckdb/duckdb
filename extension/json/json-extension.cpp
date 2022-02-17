#define DUCKDB_EXTENSION_MAIN
#include "json-extension.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_functions.hpp"

namespace duckdb {

void JSONExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);
	for (auto &fun : JSONFunctions::GetFunctions()) {
		catalog.CreateFunction(*con.context, &fun);
	}

	con.Query("CREATE MACRO json_group_array(x) AS json_quote(list(x));");
	con.Query("CREATE MACRO json_group_object(name, value) AS json_quote(map(list(name), list(value)));");
	con.Query("CREATE MACRO json(x) as json_extract(x, '$');");

	con.Commit();
}

std::string JSONExtension::Name() {
	return "json";
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void json_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::JSONExtension>();
}

DUCKDB_EXTENSION_API const char *json_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
