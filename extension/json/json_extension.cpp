#include "json_extension.hpp"

#include "json_common.hpp"
#include "json_functions.hpp"

#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"
#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/function/copy_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/parser/expression/function_expression.hpp"

namespace duckdb {

static const DefaultMacro JSON_MACROS[] = {
    {DEFAULT_SCHEMA,
     "json_group_array",
     {"x", nullptr},
     {{nullptr, nullptr}},
     "CAST('[' || string_agg(CASE WHEN x IS NULL THEN 'null'::JSON ELSE to_json(x) END, ',') || ']' AS JSON)"},
    {DEFAULT_SCHEMA,
     "json_group_object",
     {"n", "v", nullptr},
     {{nullptr, nullptr}},
     "CAST('{' || string_agg(to_json(n::VARCHAR) || ':' || CASE WHEN v IS NULL THEN 'null'::JSON ELSE to_json(v) END, "
     "',') || '}' AS JSON)"},
    {DEFAULT_SCHEMA,
     "json_group_structure",
     {"x", nullptr},
     {{nullptr, nullptr}},
     "json_structure(json_group_array(x))->0"},
    {DEFAULT_SCHEMA, "json", {"x", nullptr}, {{nullptr, nullptr}}, "json_extract(x, '$')"},
    {nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}};

static void LoadInternal(ExtensionLoader &loader) {
	// JSON type
	auto json_type = LogicalType::JSON();
	loader.RegisterType(LogicalType::JSON_TYPE_NAME, std::move(json_type));

	// JSON casts
	JSONFunctions::RegisterSimpleCastFunctions(loader);
	JSONFunctions::RegisterJSONCreateCastFunctions(loader);
	JSONFunctions::RegisterJSONTransformCastFunctions(loader);

	// JSON scalar functions
	for (auto &fun : JSONFunctions::GetScalarFunctions()) {
		loader.RegisterFunction(fun);
	}

	// JSON table functions
	for (auto &fun : JSONFunctions::GetTableFunctions()) {
		loader.RegisterFunction(fun);
	}

	// JSON pragma functions
	for (auto &fun : JSONFunctions::GetPragmaFunctions()) {
		loader.RegisterFunction(fun);
	}

	// JSON replacement scan
	DBConfig::GetConfig(loader.GetDatabaseInstance())
	    .replacement_scans.emplace_back(JSONFunctions::ReadJSONReplacement);

	// JSON copy function
	auto copy_fun = JSONFunctions::GetJSONCopyFunction();
	loader.RegisterFunction(copy_fun);
	copy_fun.extension = "ndjson";
	copy_fun.name = "ndjson";
	loader.RegisterFunction(copy_fun);
	copy_fun.extension = "jsonl";
	copy_fun.name = "jsonl";
	loader.RegisterFunction(copy_fun);

	// JSON macro's
	for (idx_t index = 0; JSON_MACROS[index].name != nullptr; index++) {
		auto info = DefaultFunctionGenerator::CreateInternalMacroInfo(JSON_MACROS[index]);
		loader.RegisterFunction(*info);
	}
}

void JsonExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
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

DUCKDB_CPP_EXTENSION_ENTRY(json, loader) {
	duckdb::LoadInternal(loader);
}
}
