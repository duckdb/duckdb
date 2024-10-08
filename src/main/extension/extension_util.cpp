#include "duckdb/main/extension_util.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"

namespace duckdb {

void ExtensionUtil::InitializeExtension(DatabaseInstance &db) {
	// Extensions have a "copy" of DuckDB statically compiled into them, so they might have a copy of jemalloc
	// Therefore, their DEFAULT_ALLOCATION_FUNCTIONS in stl_allocator.hpp are DIFFERENT from the DuckDB binary!
	// This method gets the function pointers from the DBConfig (which are from the main DuckDB binary),
	// and overwrites the DEFAULT_ALLOCATION_FUNCTIONS that were statically compiled into extension.
	// This makes it so that allocations in extensions use the same jemalloc!
	// This is very important because we'd rather not initialize jemalloc once for every dynamically loaded extension
	// Important notes:
	// 1. This function should only be called from WITHIN extensions! Otherwise, it has no effect.
	// 2. This should be the FIRST thing that extensions call in their Load()
	// 3. Static containers will be allocated before this function is called, so avoid them in extensions,
	//    otherwise we will still end up initializing jemalloc within the dynamically loaded extension.
	//    For these, we have created static_vector, static_unordered_map, etc. which do not use jemalloc
	auto &config = DBConfig::GetConfig(db);
	DEFAULT_ALLOCATION_FUNCTIONS = config.allocation_functions;
}

void ExtensionUtil::RegisterExtension(DatabaseInstance &db, const string &name,
                                      const ExtensionLoadedInfo &description) {

	db.AddExtensionInfo(name, description);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, ScalarFunction function) {
	ScalarFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, ScalarFunctionSet set) {
	CreateScalarFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(db, std::move(info));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, CreateScalarFunctionInfo info) {
	D_ASSERT(!info.functions.name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, AggregateFunction function) {
	AggregateFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, AggregateFunctionSet set) {
	CreateAggregateFunctionInfo info(std::move(set));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(db, std::move(info));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, CreateAggregateFunctionInfo info) {
	D_ASSERT(!info.functions.name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, CreateSecretFunction function) {
	D_ASSERT(!function.secret_type.empty());
	auto &config = DBConfig::GetConfig(db);
	config.secret_manager->RegisterSecretFunction(std::move(function), OnCreateConflict::ERROR_ON_CONFLICT);
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, TableFunction function) {
	TableFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(db, std::move(set));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, TableFunctionSet function) {
	D_ASSERT(!function.name.empty());
	CreateTableFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(db, std::move(info));
}

void ExtensionUtil::RegisterFunction(DatabaseInstance &db, CreateTableFunctionInfo info) {
	D_ASSERT(!info.functions.name.empty());
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

void ExtensionUtil::RegisterCollation(DatabaseInstance &db, CreateCollationInfo &info) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	system_catalog.CreateCollation(data, info);

	// Also register as a function for serialisation
	CreateScalarFunctionInfo finfo(info.function);
	finfo.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	system_catalog.CreateFunction(data, finfo);
}

void ExtensionUtil::AddFunctionOverload(DatabaseInstance &db, ScalarFunction function) {
	auto &scalar_function = ExtensionUtil::GetFunction(db, function.name);
	scalar_function.functions.AddFunction(std::move(function));
}

void ExtensionUtil::AddFunctionOverload(DatabaseInstance &db, ScalarFunctionSet functions) { // NOLINT
	D_ASSERT(!functions.name.empty());
	auto &scalar_function = ExtensionUtil::GetFunction(db, functions.name);
	for (auto &function : functions.functions) {
		function.name = functions.name;
		scalar_function.functions.AddFunction(std::move(function));
	}
}

void ExtensionUtil::AddFunctionOverload(DatabaseInstance &db, TableFunctionSet functions) { // NOLINT
	auto &table_function = ExtensionUtil::GetTableFunction(db, functions.name);
	for (auto &function : functions.functions) {
		function.name = functions.name;
		table_function.functions.AddFunction(std::move(function));
	}
}

optional_ptr<CatalogEntry> TryGetEntry(DatabaseInstance &db, const string &name, CatalogType type) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	return schema.GetEntry(data, type, name);
}

optional_ptr<CatalogEntry> ExtensionUtil::TryGetFunction(DatabaseInstance &db, const string &name) {
	return TryGetEntry(db, name, CatalogType::SCALAR_FUNCTION_ENTRY);
}

ScalarFunctionCatalogEntry &ExtensionUtil::GetFunction(DatabaseInstance &db, const string &name) {
	auto catalog_entry = TryGetFunction(db, name);
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"%s\" not found in ExtensionUtil::GetFunction", name);
	}
	return catalog_entry->Cast<ScalarFunctionCatalogEntry>();
}

optional_ptr<CatalogEntry> ExtensionUtil::TryGetTableFunction(DatabaseInstance &db, const string &name) {
	return TryGetEntry(db, name, CatalogType::TABLE_FUNCTION_ENTRY);
}

TableFunctionCatalogEntry &ExtensionUtil::GetTableFunction(DatabaseInstance &db, const string &name) {
	auto catalog_entry = TryGetTableFunction(db, name);
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"%s\" not found in ExtensionUtil::GetTableFunction", name);
	}
	return catalog_entry->Cast<TableFunctionCatalogEntry>();
}

void ExtensionUtil::RegisterType(DatabaseInstance &db, string type_name, LogicalType type,
                                 bind_type_modifiers_function_t bind_modifiers) {
	D_ASSERT(!type_name.empty());
	CreateTypeInfo info(std::move(type_name), std::move(type), bind_modifiers);
	info.temporary = true;
	info.internal = true;
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateType(data, info);
}

void ExtensionUtil::RegisterSecretType(DatabaseInstance &db, SecretType secret_type) {
	auto &config = DBConfig::GetConfig(db);
	config.secret_manager->RegisterSecretType(secret_type);
}

void ExtensionUtil::RegisterCastFunction(DatabaseInstance &db, const LogicalType &source, const LogicalType &target,
                                         BoundCastInfo function, int64_t implicit_cast_cost) {
	auto &config = DBConfig::GetConfig(db);
	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(source, target, std::move(function), implicit_cast_cost);
}

} // namespace duckdb
