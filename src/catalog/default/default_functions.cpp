#include "duckdb/catalog/default/default_functions.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/expression/columnref_expression.hpp"
#include "duckdb/catalog/catalog_entry/macro_catalog_entry.hpp"

namespace duckdb {

struct DefaultMacro {
	const char *name;
	const char *parameters[8];
	const char *macro;
};

static DefaultMacro internal_macros[] = {{"nullif", {"a", "b", nullptr}, "CASE WHEN a=b THEN NULL ELSE a END"},
                                         {nullptr, {nullptr}, nullptr}};

static unique_ptr<CreateFunctionInfo> GetDefaultFunction(const string &schema, const string &name) {
	for (idx_t index = 0; internal_macros[index].name != nullptr; index++) {
		if (internal_macros[index].name == name) {
			// parse the expression
			auto expressions = Parser::ParseExpressionList(internal_macros[index].macro);
			D_ASSERT(expressions.size() == 1);

			auto result = make_unique<MacroFunction>(move(expressions[0]));
			for (idx_t param_idx = 0; internal_macros[index].parameters[param_idx] != nullptr; param_idx++) {
				result->parameters.push_back(
				    make_unique<ColumnRefExpression>(internal_macros[index].parameters[param_idx]));
			}

			auto bind_info = make_unique<CreateMacroInfo>();
			bind_info->schema = DEFAULT_SCHEMA;
			bind_info->name = internal_macros[index].name;
			bind_info->temporary = true;
			bind_info->internal = true;
			bind_info->function = move(result);
			return move(bind_info);
		}
	}
	return nullptr;
}

DefaultFunctionGenerator::DefaultFunctionGenerator(Catalog &catalog, SchemaCatalogEntry *schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CatalogEntry> DefaultFunctionGenerator::CreateDefaultEntry(ClientContext &context,
                                                                      const string &entry_name) {
	auto info = GetDefaultFunction(schema->name, entry_name);
	if (info) {
		return make_unique_base<CatalogEntry, MacroCatalogEntry>(&catalog, schema, (CreateMacroInfo *)info.get());
	}
	return nullptr;
}

} // namespace duckdb
