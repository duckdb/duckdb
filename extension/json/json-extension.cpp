#define DUCKDB_EXTENSION_MAIN
#include "json-extension.hpp"

#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "duckdb/common/string_util.hpp"
#include "json_functions.hpp"

namespace duckdb {

static DefaultMacro json_macros[] = {
    {DEFAULT_SCHEMA, "json_group_array", {"x", nullptr}, "to_json(list(x))"},
    {DEFAULT_SCHEMA, "json_group_object", {"name", "value", nullptr}, "to_json(map(list(name), list(value)))"},
    {DEFAULT_SCHEMA, "json_group_structure", {"x", nullptr}, "json_structure(json_group_array(x))->0"},
    {DEFAULT_SCHEMA, "json", {"x", nullptr}, "json_extract(x, '$')"},
    {nullptr, nullptr, {nullptr}, nullptr}};

static DefaultMacro table_macros[] = {
    {DEFAULT_SCHEMA,
     "read_json_objects",
     {"json_file", nullptr},
     "SELECT * FROM read_csv(json_file, columns={'json': 'JSON'}, delim=NULL, header=0, quote=NULL, escape=NULL)"},
    {DEFAULT_SCHEMA, "read_ndjson_objects", {"json_file", nullptr}, "SELECT * FROM read_json_objects(json_file)"},
    {nullptr, nullptr, {nullptr}, nullptr}};

void JSONExtension::Load(DuckDB &db) {
	Connection con(db);
	con.BeginTransaction();

	auto &catalog = Catalog::GetCatalog(*con.context);
	for (auto &fun : JSONFunctions::GetFunctions()) {
		catalog.CreateFunction(*con.context, &fun);
	}

	for (idx_t index = 0; json_macros[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(json_macros[index]);
		catalog.CreateFunction(*con.context, info.get());
	}
	for (idx_t index = 0; table_macros[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalTableMacroInfo(table_macros[index]);
		catalog.CreateFunction(*con.context, info.get());
	}
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
