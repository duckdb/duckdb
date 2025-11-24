#include "duckdb/catalog/default/default_table_functions.hpp"
#include "duckdb/catalog/catalog_entry/table_macro_catalog_entry.hpp"
#include "duckdb/parser/parser.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/function/table_macro_function.hpp"

namespace duckdb {

// clang-format off
static const DefaultTableMacro internal_table_macros[] = {
	{DEFAULT_SCHEMA, "histogram_values", {"source", "col_name", nullptr}, {{"bin_count", "10"}, {"technique", "'auto'"}, {nullptr, nullptr}},  R"(
WITH bins AS (
   SELECT
      CASE
      WHEN (NOT (can_cast_implicitly(MIN(col_name), NULL::BIGINT) OR
            can_cast_implicitly(MIN(col_name), NULL::DOUBLE) OR
            can_cast_implicitly(MIN(col_name), NULL::TIMESTAMP)) AND technique='auto')
            OR technique='sample'
      THEN
         approx_top_k(col_name, bin_count)
      WHEN technique='equi-height'
      THEN
         quantile(col_name, [x / bin_count::DOUBLE for x in generate_series(1, bin_count)])
      WHEN technique='equi-width'
      THEN
         equi_width_bins(MIN(col_name), MAX(col_name), bin_count, false)
      WHEN technique='equi-width-nice' OR technique='auto'
      THEN
         equi_width_bins(MIN(col_name), MAX(col_name), bin_count, true)
      ELSE
         error(concat('Unrecognized technique ', technique))
      END AS bins
   FROM query_table(source::VARCHAR)
   )
SELECT UNNEST(map_keys(histogram)) AS bin, UNNEST(map_values(histogram)) AS count
FROM (
   SELECT CASE
      WHEN (NOT (can_cast_implicitly(MIN(col_name), NULL::BIGINT) OR
            can_cast_implicitly(MIN(col_name), NULL::DOUBLE) OR
            can_cast_implicitly(MIN(col_name), NULL::TIMESTAMP)) AND technique='auto')
            OR technique='sample'
      THEN
            histogram_exact(col_name, bins)
      ELSE
            histogram(col_name, bins)
      END AS histogram
   FROM query_table(source::VARCHAR), bins
);
)"},
	{DEFAULT_SCHEMA, "histogram", {"source", "col_name", nullptr}, {{"bin_count", "10"}, {"technique", "'auto'"}, {nullptr, nullptr}},  R"(
SELECT
   CASE
   WHEN is_histogram_other_bin(bin)
   THEN '(other values)'
   WHEN (NOT (can_cast_implicitly(bin, NULL::BIGINT) OR
              can_cast_implicitly(bin, NULL::DOUBLE) OR
              can_cast_implicitly(bin, NULL::TIMESTAMP)) AND technique='auto')
              OR technique='sample'
   THEN bin::VARCHAR
   WHEN row_number() over () = 1
   THEN concat('x <= ', bin::VARCHAR)
   ELSE concat(lag(bin::VARCHAR) over (), ' < x <= ', bin::VARCHAR)
   END AS bin,
   count,
   bar(count, 0, max(count) over ()) AS bar
FROM histogram_values(source, col_name, bin_count := bin_count, technique := technique);
)"},
	{DEFAULT_SCHEMA, "duckdb_logs_parsed", {"log_type"}, {}, R"(
SELECT * EXCLUDE (message), UNNEST(parse_duckdb_log_message(log_type, message))
FROM duckdb_logs(denormalized_table=1)
WHERE type ILIKE log_type
)"},
	{nullptr, nullptr, {nullptr}, {{nullptr, nullptr}}, nullptr}
	};
// clang-format on

DefaultTableFunctionGenerator::DefaultTableFunctionGenerator(Catalog &catalog, SchemaCatalogEntry &schema)
    : DefaultGenerator(catalog), schema(schema) {
}

unique_ptr<CreateMacroInfo>
DefaultTableFunctionGenerator::CreateInternalTableMacroInfo(const DefaultTableMacro &default_macro,
                                                            unique_ptr<MacroFunction> function) {
	for (idx_t param_idx = 0; default_macro.parameters[param_idx] != nullptr; param_idx++) {
		function->parameters.push_back(make_uniq<ColumnRefExpression>(default_macro.parameters[param_idx]));
	}
	for (idx_t named_idx = 0; default_macro.named_parameters[named_idx].name != nullptr; named_idx++) {
		const auto &named_param = default_macro.named_parameters[named_idx];
		auto expr_list = Parser::ParseExpressionList(named_param.default_value);
		if (expr_list.size() != 1) {
			throw InternalException("Expected a single expression");
		}
		function->parameters.push_back(make_uniq<ColumnRefExpression>(named_param.name));
		function->default_parameters.insert(make_pair(named_param.name, std::move(expr_list[0])));
	}

	auto type = CatalogType::TABLE_MACRO_ENTRY;
	auto bind_info = make_uniq<CreateMacroInfo>(type);
	bind_info->schema = default_macro.schema;
	bind_info->name = default_macro.name;
	bind_info->temporary = true;
	bind_info->internal = true;
	bind_info->macros.push_back(std::move(function));
	return bind_info;
}

unique_ptr<CreateMacroInfo>
DefaultTableFunctionGenerator::CreateTableMacroInfo(const DefaultTableMacro &default_macro) {
	Parser parser;
	parser.ParseQuery(default_macro.macro);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw InternalException("Expected a single select statement in CreateTableMacroInfo internal");
	}
	auto node = std::move(parser.statements[0]->Cast<SelectStatement>().node);

	auto result = make_uniq<TableMacroFunction>(std::move(node));
	return CreateInternalTableMacroInfo(default_macro, std::move(result));
}

static unique_ptr<CreateFunctionInfo> GetDefaultTableFunction(const string &input_schema, const string &input_name) {
	auto schema = StringUtil::Lower(input_schema);
	auto name = StringUtil::Lower(input_name);
	for (idx_t index = 0; internal_table_macros[index].name != nullptr; index++) {
		if (internal_table_macros[index].schema == schema && internal_table_macros[index].name == name) {
			return DefaultTableFunctionGenerator::CreateTableMacroInfo(internal_table_macros[index]);
		}
	}
	return nullptr;
}

unique_ptr<CatalogEntry> DefaultTableFunctionGenerator::CreateDefaultEntry(ClientContext &context,
                                                                           const string &entry_name) {
	auto info = GetDefaultTableFunction(schema.name, entry_name);
	if (info) {
		return make_uniq_base<CatalogEntry, TableMacroCatalogEntry>(catalog, schema, info->Cast<CreateMacroInfo>());
	}
	return nullptr;
}

vector<string> DefaultTableFunctionGenerator::GetDefaultEntries() {
	vector<string> result;
	for (idx_t index = 0; internal_table_macros[index].name != nullptr; index++) {
		if (StringUtil::Lower(internal_table_macros[index].name) != internal_table_macros[index].name) {
			throw InternalException("Default macro name %s should be lowercase", internal_table_macros[index].name);
		}
		if (internal_table_macros[index].schema == schema.name) {
			result.emplace_back(internal_table_macros[index].name);
		}
	}
	return result;
}

} // namespace duckdb
