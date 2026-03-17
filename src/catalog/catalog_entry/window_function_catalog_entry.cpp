#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_window_function_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"

namespace duckdb {

WindowFunctionCatalogEntry::WindowFunctionCatalogEntry(Catalog &catalog, SchemaCatalogEntry &schema,
                                                       CreateWindowFunctionInfo &info)
    : FunctionEntry(Type, catalog, schema, info), functions(info.functions) {
	for (auto &function : functions.functions) {
		function.catalog_name = catalog.GetAttached().GetName();
		function.schema_name = schema.name;
	}
}

vector<unique_ptr<WindowFunctionCatalogEntry>> WindowFunctionCatalogEntry::GetEntries(ClientContext &context) {
	auto &catalog = Catalog::GetSystemCatalog(DatabaseInstance::GetDatabase(context));
	string schema_name(DEFAULT_SCHEMA);
	EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, schema_name);
	auto &schema = catalog.GetSchema(context, schema_lookup);

	vector<unique_ptr<WindowFunctionCatalogEntry>> entries;

	{
		WindowFunction fun("row_number", {}, LogicalType::BIGINT);
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("rank", {}, LogicalType::BIGINT);
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("dense_rank", {}, LogicalType::BIGINT);
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("rank_dense", {}, LogicalType::BIGINT);
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("ntile", {LogicalType::BIGINT}, LogicalType::BIGINT);
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("percent_rank", {}, LogicalType::DOUBLE);
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("cume_dist", {}, LogicalType::DOUBLE);
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("first_value", {LogicalType::TEMPLATE("T")}, LogicalType::TEMPLATE("T"));
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("first", {LogicalType::TEMPLATE("T")}, LogicalType::TEMPLATE("T"));
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("last_value", {LogicalType::TEMPLATE("T")}, LogicalType::TEMPLATE("T"));
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("last", {LogicalType::TEMPLATE("T")}, LogicalType::TEMPLATE("T"));
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("nth_value", {LogicalType::TEMPLATE("T"), LogicalType::BIGINT}, LogicalType::TEMPLATE("T"));
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("lead", {LogicalType::TEMPLATE("T"), LogicalType::BIGINT, LogicalType::TEMPLATE("T")},
		                   LogicalType::TEMPLATE("T"));
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("lag", {LogicalType::TEMPLATE("T"), LogicalType::BIGINT, LogicalType::TEMPLATE("T")},
		                   LogicalType::TEMPLATE("T"));
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		WindowFunction fun("fill", {LogicalType::TEMPLATE("T")}, LogicalType::TEMPLATE("T"));
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	return entries;
}

} // namespace duckdb
