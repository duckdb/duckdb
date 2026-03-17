#include "duckdb/catalog/catalog_entry/window_function_catalog_entry.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/parsed_data/create_window_function_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/main/attached_database.hpp"

#include "duckdb/function/window/window_rank_function.hpp"
#include "duckdb/function/window/window_rownumber_function.hpp"
#include "duckdb/function/window/window_value_function.hpp"

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
		CreateWindowFunctionInfo info(RowNumberFunc::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(RankFunc::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(DenseRankFun::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(RankDenseFun::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(NtileFunc::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(PercentRankFun::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(CumeDistFun::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(FirstValueFun::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		//	first is also an aggregate...
		auto fun = FirstValueFun::GetFunction();
		fun.name = "first";
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(LastValueFun::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		//	last is also an aggregate...
		auto fun = LastValueFun::GetFunction();
		fun.name = "last";
		CreateWindowFunctionInfo info(fun);
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(NthValueFun::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(LeadFun::GetFunctions());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(LagFun::GetFunctions());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	{
		CreateWindowFunctionInfo info(FillFun::GetFunction());
		entries.emplace_back(make_uniq<WindowFunctionCatalogEntry>(catalog, schema, info));
	}

	return entries;
}

} // namespace duckdb
