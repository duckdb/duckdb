#define DUCKDB_EXTENSION_MAIN
#include "json_extension.hpp"

#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include "duckdb/parser/expression/constant_expression.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "json_common.hpp"
#include "json_functions.hpp"

namespace duckdb {

static DefaultMacro json_macros[] = {
    {DEFAULT_SCHEMA, "json_group_array", {"x", nullptr}, "to_json(list(x))"},
    {DEFAULT_SCHEMA, "json_group_object", {"name", "value", nullptr}, "to_json(map(list(name), list(value)))"},
    {DEFAULT_SCHEMA, "json_group_structure", {"x", nullptr}, "json_structure(json_group_array(x))->'0'"},
    {DEFAULT_SCHEMA, "json", {"x", nullptr}, "json_extract(x, '$')"},
    {nullptr, nullptr, {nullptr}, nullptr}};

void JsonExtension::Load(DuckDB &db) {
	auto &db_instance = *db.instance;
	// JSON type
	auto json_type = LogicalType::JSON();
	ExtensionUtil::RegisterType(db_instance, LogicalType::JSON_TYPE_NAME, std::move(json_type));

	// JSON casts
	JSONFunctions::RegisterSimpleCastFunctions(DBConfig::GetConfig(db_instance).GetCastFunctions());
	JSONFunctions::RegisterJSONCreateCastFunctions(DBConfig::GetConfig(db_instance).GetCastFunctions());
	JSONFunctions::RegisterJSONTransformCastFunctions(DBConfig::GetConfig(db_instance).GetCastFunctions());

	// JSON scalar functions
	for (auto &fun : JSONFunctions::GetScalarFunctions()) {
		ExtensionUtil::RegisterFunction(db_instance, fun);
	}

	// JSON table functions
	for (auto &fun : JSONFunctions::GetTableFunctions()) {
		ExtensionUtil::RegisterFunction(db_instance, fun);
	}

	// JSON pragma functions
	for (auto &fun : JSONFunctions::GetPragmaFunctions()) {
		ExtensionUtil::RegisterFunction(db_instance, fun);
	}

	// JSON replacement scan
	auto &config = DBConfig::GetConfig(*db.instance);
	config.replacement_scans.emplace_back(JSONFunctions::ReadJSONReplacement);

	// JSON copy function
	auto copy_fun = JSONFunctions::GetJSONCopyFunction();
	ExtensionUtil::RegisterFunction(db_instance, std::move(copy_fun));

	// JSON macro's
	for (idx_t index = 0; json_macros[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(json_macros[index]);
		ExtensionUtil::RegisterFunction(db_instance, *info);
	}
}

std::string JsonExtension::Name() {
	return "json";
}

std::string JsonExtension::Version() const {
#ifdef EXT_VERSION_JSON
	return EXT_VERSION_JSON;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void json_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::JsonExtension>();
}

DUCKDB_EXTENSION_API const char *json_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
