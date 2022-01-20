#include "json-extension.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "json_functions.hpp"

namespace duckdb {

void JSONExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	JSONFunctions::AddFunctions(*con.context);

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
