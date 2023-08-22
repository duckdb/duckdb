#include "duckdb/main/extension_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, ScalarFunctionSet set) {
	D_ASSERT(!set.name.empty());
	CreateScalarFunctionInfo info(std::move(set));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, ScalarFunction function) {
	D_ASSERT(!function.name.empty());
	ScalarFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, AggregateFunction function) {
	D_ASSERT(!function.name.empty());
	AggregateFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, AggregateFunctionSet set) {
	D_ASSERT(!set.name.empty());
	CreateAggregateFunctionInfo info(std::move(set));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, TableFunction function) {
	D_ASSERT(!function.name.empty());
	TableFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, TableFunctionSet function) {
	D_ASSERT(!function.name.empty());
	CreateTableFunctionInfo info(std::move(function));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, PragmaFunction function) {
	D_ASSERT(!function.name.empty());
	PragmaFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, PragmaFunctionSet function) {
	D_ASSERT(!function.name.empty());
	auto function_name = function.name;
	CreatePragmaFunctionInfo info(std::move(function_name), std::move(function));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreatePragmaFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, CopyFunction function) {
	CreateCopyFunctionInfo info(std::move(function));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateCopyFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, CreateMacroInfo &info) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterType(DatabaseInstance &db, string type_name, LogicalType type) {
	D_ASSERT(!type_name.empty());
	CreateTypeInfo info(std::move(type_name), std::move(type));
	info.temporary = true;
	info.internal = true;
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateType(data, info);
}

void ExtensionUtil::RegisterCastFunction(DatabaseInstance &db, const LogicalType &source, const LogicalType &target,
                                         BoundCastInfo function, int64_t implicit_cast_cost) {
	auto &config = DBConfig::GetConfig(db);
	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(source, target, std::move(function), implicit_cast_cost);
}

} // namespace duckdb
