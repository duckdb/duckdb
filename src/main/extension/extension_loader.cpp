#include "duckdb/main/extension/extension_loader.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

ExtensionLoader::ExtensionLoader(ExtensionActiveLoad &load_info)
    : db(load_info.db), extension_name(load_info.extension_name), extension_info(load_info.info) {
}

ExtensionLoader::ExtensionLoader(DatabaseInstance &db, const string &name) : db(db), extension_name(name) {
}

DatabaseInstance &ExtensionLoader::GetDatabaseInstance() {
	return db;
}

void ExtensionLoader::SetDescription(const string &description) {
	extension_description = description;
}

void ExtensionLoader::FinalizeLoad() {
	// Set extension description, if provided
	if (!extension_description.empty() && extension_info) {
		auto info = make_uniq<ExtensionLoadedInfo>();
		info->description = extension_description;
		extension_info->load_info = std::move(info);
	}
}

void ExtensionLoader::RegisterFunction(ScalarFunction function) {
	ScalarFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(std::move(set));
}

void ExtensionLoader::RegisterFunction(ScalarFunctionSet function) {
	CreateScalarFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(std::move(info));
}

void ExtensionLoader::RegisterFunction(CreateScalarFunctionInfo function) {
	D_ASSERT(!function.functions.name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, function);
}

void ExtensionLoader::RegisterFunction(AggregateFunction function) {
	AggregateFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(std::move(set));
}

void ExtensionLoader::RegisterFunction(AggregateFunctionSet function) {
	CreateAggregateFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(std::move(info));
}

void ExtensionLoader::RegisterFunction(CreateAggregateFunctionInfo function) {
	D_ASSERT(!function.functions.name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, function);
}

void ExtensionLoader::RegisterFunction(CreateSecretFunction function) {
	D_ASSERT(!function.secret_type.empty());
	auto &config = DBConfig::GetConfig(db);
	config.secret_manager->RegisterSecretFunction(std::move(function), OnCreateConflict::ERROR_ON_CONFLICT);
}

void ExtensionLoader::RegisterFunction(TableFunction function) {
	TableFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(std::move(set));
}

void ExtensionLoader::RegisterFunction(TableFunctionSet function) {
	D_ASSERT(!function.name.empty());
	CreateTableFunctionInfo info(std::move(function));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(std::move(info));
}

void ExtensionLoader::RegisterFunction(CreateTableFunctionInfo info) {
	D_ASSERT(!info.functions.name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionLoader::RegisterFunction(PragmaFunction function) {
	D_ASSERT(!function.name.empty());
	PragmaFunctionSet set(function.name);
	set.AddFunction(std::move(function));
	RegisterFunction(std::move(set));
}

void ExtensionLoader::RegisterFunction(PragmaFunctionSet function) {
	D_ASSERT(!function.name.empty());
	auto function_name = function.name;
	CreatePragmaFunctionInfo info(std::move(function_name), std::move(function));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreatePragmaFunction(data, info);
}

void ExtensionLoader::RegisterFunction(CopyFunction function) {
	CreateCopyFunctionInfo info(std::move(function));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateCopyFunction(data, info);
}

void ExtensionLoader::RegisterFunction(CreateMacroInfo &info) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionLoader::RegisterCollation(CreateCollationInfo &info) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	system_catalog.CreateCollation(data, info);

	// Also register as a function for serialisation
	CreateScalarFunctionInfo finfo(info.function);
	finfo.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	system_catalog.CreateFunction(data, finfo);
}

void ExtensionLoader::RegisterCoordinateSystem(CreateCoordinateSystemInfo &info) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateCoordinateSystem(data, info);
}

void ExtensionLoader::AddFunctionOverload(ScalarFunction function) {
	auto &scalar_function = GetFunction(function.name);
	scalar_function.functions.AddFunction(std::move(function));
}

void ExtensionLoader::AddFunctionOverload(ScalarFunctionSet functions) { // NOLINT
	D_ASSERT(!functions.name.empty());
	auto &scalar_function = GetFunction(functions.name);
	for (auto &function : functions.functions) {
		function.name = functions.name;
		scalar_function.functions.AddFunction(std::move(function));
	}
}

void ExtensionLoader::AddFunctionOverload(TableFunctionSet functions) { // NOLINT
	auto &table_function = GetTableFunction(functions.name);
	for (auto &function : functions.functions) {
		function.name = functions.name;
		table_function.functions.AddFunction(std::move(function));
	}
}

static optional_ptr<CatalogEntry> TryGetEntry(DatabaseInstance &db, const string &name, CatalogType type) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, DEFAULT_SCHEMA);
	return schema.GetEntry(data, type, name);
}

optional_ptr<CatalogEntry> ExtensionLoader::TryGetFunction(const string &name) {
	return TryGetEntry(db, name, CatalogType::SCALAR_FUNCTION_ENTRY);
}

ScalarFunctionCatalogEntry &ExtensionLoader::GetFunction(const string &name) {
	auto catalog_entry = TryGetFunction(name);
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"%s\" not found in ExtensionLoader::GetFunction", name);
	}
	return catalog_entry->Cast<ScalarFunctionCatalogEntry>();
}

optional_ptr<CatalogEntry> ExtensionLoader::TryGetTableFunction(const string &name) {
	return TryGetEntry(db, name, CatalogType::TABLE_FUNCTION_ENTRY);
}

TableFunctionCatalogEntry &ExtensionLoader::GetTableFunction(const string &name) {
	auto catalog_entry = TryGetTableFunction(name);
	if (!catalog_entry) {
		throw InvalidInputException("Function with name \"%s\" not found in ExtensionLoader::GetTableFunction", name);
	}
	return catalog_entry->Cast<TableFunctionCatalogEntry>();
}

void ExtensionLoader::RegisterType(string type_name, LogicalType type, bind_logical_type_function_t bind_modifiers) {
	D_ASSERT(!type_name.empty());
	CreateTypeInfo info(std::move(type_name), std::move(type), bind_modifiers);
	info.temporary = true;
	info.internal = true;
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateType(data, info);
}

void ExtensionLoader::RegisterSecretType(SecretType secret_type) {
	auto &config = DBConfig::GetConfig(db);
	config.secret_manager->RegisterSecretType(secret_type);
}

void ExtensionLoader::RegisterCastFunction(const LogicalType &source, const LogicalType &target,
                                           bind_cast_function_t bind_function, int64_t implicit_cast_cost) {
	auto &config = DBConfig::GetConfig(db);
	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(source, target, bind_function, implicit_cast_cost);
}

void ExtensionLoader::RegisterCastFunction(const LogicalType &source, const LogicalType &target, BoundCastInfo function,
                                           int64_t implicit_cast_cost) {
	auto &config = DBConfig::GetConfig(db);
	auto &casts = config.GetCastFunctions();
	casts.RegisterCastFunction(source, target, std::move(function), implicit_cast_cost);
}

} // namespace duckdb
