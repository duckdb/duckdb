#include "duckdb/main/extension/extension_loader.hpp"

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_window_function_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/main/extension_install_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/secret/secret_manager.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/profiler/metrics_manager.hpp"

#include "duckdb/main/extension_callback_manager.hpp"
#include "re2/re2.h"

namespace duckdb {

ExtensionLoader::ExtensionLoader(const ExtensionActiveLoad &load_info)
    : db(load_info.db), extension_info(load_info.info) {
	loader_info.extension_name = load_info.extension_name;
	loader_info.extension_alias = load_info.alias;
}

ExtensionLoader::ExtensionLoader(DatabaseInstance &db, const string &name) : db(db) {
	loader_info.extension_name = Identifier(name);
}

DatabaseInstance &ExtensionLoader::GetDatabaseInstance() const {
	return db;
}

void ExtensionLoader::SetDescription(const string &description) {
	loader_info.extension_description = description;
}

void ExtensionLoader::UseDedicatedSchemaForExtension(const Identifier &extension_schema_name) {
	CreateSchema(extension_schema_name);
	UseDefaultSchema(extension_schema_name);
	AddSchemaToSearchPath(extension_schema_name);
}

void ExtensionLoader::UseDedicatedSchemaForExtension() {
	auto registered_ext_name = GetRegisteredExtensionName();
	UseDedicatedSchemaForExtension(registered_ext_name);
}

void ExtensionLoader::CreateSchema(const Identifier &name) const {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);

	CreateSchemaInfo info;
	info.SetQualifiedName(QualifiedName(info.GetQualifiedName().Catalog(), name, info.GetQualifiedName().Name()));
	info.internal = true;
	// TODO; we can give the user more control here
	info.on_conflict = OnCreateConflict::ERROR_ON_CONFLICT;
	system_catalog.CreateSchema(data, info);
}

void ExtensionLoader::UseDefaultSchema(const Identifier &name) {
	if (loader_info.extension_schema != DEFAULT_SCHEMA && name != DEFAULT_SCHEMA &&
	    loader_info.extension_schema != name) {
		throw InvalidInputException("Cannot set extension schema to %s, schema is already set to %s", name,
		                            loader_info.extension_schema);
	}
	if (name == "pg_catalog") {
		throw InvalidInputException("Cannot set default extension schema to %s", name);
	}
	if (name == DEFAULT_SCHEMA) {
		loader_info.extension_schema = Identifier::DefaultSchema();
		return;
	}
	loader_info.extension_schema = name;
}

void ExtensionLoader::AddSchemaToSearchPath(const Identifier &schema_name) const {
	// adds an explicitly set extension schema to the search path
	if (loader_info.extension_schema != schema_name || schema_name == DEFAULT_SCHEMA ||
	    loader_info.extension_schema == DEFAULT_SCHEMA) {
		throw InvalidInputException("Cannot add schema %s to search path, first set the extension schema explicitly "
		                            "with UseDefaultSchema()",
		                            schema_name);
	}

	// check if schema already exists in the system catalog
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto schema = system_catalog.GetSchema(data, schema_name, OnEntryNotFound::RETURN_NULL);
	if (!schema) {
		throw InvalidInputException("Cannot add schema %s to search path: schema does not exist. "
		                            "Call CreateExtensionSchema() first.",
		                            schema_name);
	}

	// TODO: remove extension schema from search path if loading extension failed
	ExtensionCallbackManager::Get(db).AddExtensionSchema(loader_info.extension_schema);
}

void ExtensionLoader::RefreshSearchPath(ClientContext &context) {
	ClientData::Get(context).catalog_search_path->RefreshSetPaths();
}

void ExtensionLoader::FinalizeLoad() {
	// Set extension description, if provided
	if (!loader_info.extension_description.empty() && extension_info) {
		auto info = make_uniq<ExtensionLoadedInfo>();
		info->description = loader_info.extension_description;
		extension_info->load_info = std::move(info);
	}
}

void ExtensionLoader::RegisterFunction(ScalarFunction function) {
	ScalarFunctionSet set {function.name};
	set.AddFunction(std::move(function));
	RegisterFunction(std::move(set));
}

void ExtensionLoader::RegisterFunction(ScalarFunctionSet function) {
	CreateScalarFunctionInfo info(std::move(function));
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), loader_info.extension_schema, info.GetQualifiedName().Name()));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(std::move(info));
}

//! The scalar function entry of that name in a schema of the system catalog, if there is one
static optional_ptr<ScalarFunctionCatalogEntry>
TryGetScalarFunction(DatabaseInstance &db, const Identifier &schema_name, const Identifier &name) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	// UseDefaultSchema does not create the schema, so it may not exist yet
	auto schema = system_catalog.GetSchema(data, schema_name, OnEntryNotFound::RETURN_NULL);
	if (!schema) {
		return nullptr;
	}
	auto entry = schema->GetEntry(data, CatalogType::SCALAR_FUNCTION_ENTRY, name);
	if (!entry || entry->type != CatalogType::SCALAR_FUNCTION_ENTRY) {
		return nullptr;
	}
	return &entry->Cast<ScalarFunctionCatalogEntry>();
}

void ExtensionLoader::RegisterFunction(CreateScalarFunctionInfo function) {
	D_ASSERT(!function.functions.name.empty());
	auto &extension_name = GetRegisteredExtensionName();
	function.extension_name = extension_name;
	auto name = function.GetQualifiedName().Name();
	auto schema = function.GetQualifiedName().Schema();
	const bool default_schema = schema == DEFAULT_SCHEMA || schema == loader_info.extension_schema;
	bool to_main = function.functions.IsLanguage();
	if (to_main && loader_info.extension_schema != DEFAULT_SCHEMA) {
		// overloads this extension registered before the declaration are in its own schema, where member calls never
		// select them
		auto earlier = TryGetScalarFunction(db, loader_info.extension_schema, name);
		if (earlier && earlier->extension_name == extension_name) {
			throw InvalidInputException("Extension %s registered overloads of %s before declaring it a function "
			                            "language: declare the language %s before registering its overloads",
			                            extension_name, name, name);
		}
	}
	if (!to_main && default_schema) {
		// an overload added without the declaration merges into the language this extension declared, instead of
		// shadowing it. Another extension's function of that name stays in that extension's schema
		auto language = TryGetScalarFunction(db, Identifier::DefaultSchema(), name);
		to_main = language && language->functions.IsLanguage() && language->extension_name == extension_name;
	}
	if (to_main) {
		// calls of a language's members look it up in the main schema, whatever the extension's own schema is
		function.SetQualifiedName(
		    QualifiedName(function.GetQualifiedName().Catalog(), Identifier::DefaultSchema(), name));
	} else if (schema == DEFAULT_SCHEMA) {
		function.SetQualifiedName(
		    QualifiedName(function.GetQualifiedName().Catalog(), loader_info.extension_schema, name));
	}
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, function);
}

void ExtensionLoader::RegisterFunction(AggregateFunction function) {
	AggregateFunctionSet set {function.name};
	set.AddFunction(std::move(function));
	RegisterFunction(std::move(set));
}

void ExtensionLoader::RegisterFunction(AggregateFunctionSet function) {
	CreateAggregateFunctionInfo info(std::move(function));
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), loader_info.extension_schema, info.GetQualifiedName().Name()));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(std::move(info));
}

void ExtensionLoader::RegisterFunction(CreateAggregateFunctionInfo function) {
	D_ASSERT(!function.functions.name.empty());
	if (function.GetQualifiedName().Schema() == DEFAULT_SCHEMA) {
		function.SetQualifiedName(QualifiedName(function.GetQualifiedName().Catalog(), loader_info.extension_schema,
		                                        function.GetQualifiedName().Name()));
	}
	function.extension_name = GetRegisteredExtensionName();
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, function);
}

void ExtensionLoader::RegisterFunction(WindowFunction function) {
	WindowFunctionSet set {function.name};
	set.AddFunction(std::move(function));
	RegisterFunction(std::move(set));
}

void ExtensionLoader::RegisterFunction(WindowFunctionSet function) {
	CreateWindowFunctionInfo info(std::move(function));
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), loader_info.extension_schema, info.GetQualifiedName().Name()));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(std::move(info));
}

void ExtensionLoader::RegisterFunction(CreateWindowFunctionInfo function) {
	D_ASSERT(!function.functions.name.empty());
	if (function.GetQualifiedName().Schema() == DEFAULT_SCHEMA) {
		function.SetQualifiedName(QualifiedName(function.GetQualifiedName().Catalog(), loader_info.extension_schema,
		                                        function.GetQualifiedName().Name()));
	}
	function.extension_name = GetRegisteredExtensionName();
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
	TableFunctionSet set {function.name};
	set.AddFunction(std::move(function));
	RegisterFunction(std::move(set));
}

void ExtensionLoader::RegisterFunction(TableFunctionSet function) {
	D_ASSERT(!function.name.empty());
	CreateTableFunctionInfo info(std::move(function));
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), loader_info.extension_schema, info.GetQualifiedName().Name()));
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	RegisterFunction(std::move(info));
}

void ExtensionLoader::RegisterFunction(CreateTableFunctionInfo info) {
	D_ASSERT(!info.functions.name.empty());
	info.extension_name = GetRegisteredExtensionName();
	if (info.GetQualifiedName().Schema() == DEFAULT_SCHEMA) {
		info.SetQualifiedName(QualifiedName(info.GetQualifiedName().Catalog(), loader_info.extension_schema,
		                                    info.GetQualifiedName().Name()));
	}
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionLoader::RegisterFunction(PragmaFunction function) {
	D_ASSERT(!function.name.empty());
	PragmaFunctionSet set {function.name};
	set.AddFunction(std::move(function));
	RegisterFunction(std::move(set));
}

void ExtensionLoader::RegisterFunction(PragmaFunctionSet function) {
	D_ASSERT(!function.name.empty());
	RegisterFunction(CreatePragmaFunctionInfo(std::move(function)));
}

void ExtensionLoader::RegisterFunction(CreatePragmaFunctionInfo info) {
	D_ASSERT(!info.functions.name.empty());
	info.extension_name = GetRegisteredExtensionName();
	if (info.GetQualifiedName().Schema() == DEFAULT_SCHEMA) {
		info.SetQualifiedName(QualifiedName(info.GetQualifiedName().Catalog(), loader_info.extension_schema,
		                                    info.GetQualifiedName().Name()));
	}
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreatePragmaFunction(data, info);
}

void ExtensionLoader::RegisterFunction(CopyFunction function) {
	CreateCopyFunctionInfo info(std::move(function));
	info.extension_name = GetRegisteredExtensionName();
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), loader_info.extension_schema, info.GetQualifiedName().Name()));
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateCopyFunction(data, info);
}

void ExtensionLoader::RegisterFunction(CreateMacroInfo &info) {
	info.extension_name = GetRegisteredExtensionName();
	if (info.GetQualifiedName().Schema() == DEFAULT_SCHEMA) {
		info.SetQualifiedName(QualifiedName(info.GetQualifiedName().Catalog(), loader_info.extension_schema,
		                                    info.GetQualifiedName().Name()));
	}
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateFunction(data, info);
}

void ExtensionLoader::RegisterCoordinateSystem(CreateCoordinateSystemInfo &info) {
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	system_catalog.CreateCoordinateSystem(data, info);
}

void ExtensionLoader::AddFunctionOverload(ScalarFunction function) {
	auto &scalar_function = GetFunction(function.name);
	scalar_function.functions.AddFunction(scalar_function.FinalizeFunction(std::move(function)));
}

void ExtensionLoader::AddFunctionOverload(ScalarFunctionSet functions) { // NOLINT
	D_ASSERT(!functions.name.empty());
	auto &scalar_function = GetFunction(functions.name);
	for (auto &function : functions.functions) {
		function = scalar_function.FinalizeFunction(*function);
	}
	for (auto &function : functions.functions) {
		scalar_function.functions.AddFunction(std::move(function));
	}
	scalar_function.functions.MergeLanguageDeclaration(functions);
}

void ExtensionLoader::AddFunctionOverload(TableFunctionSet functions) { // NOLINT
	auto &table_function = GetTableFunction(functions.name);
	functions.ApplyToFunctions([&](TableFunction &function) { table_function.FinalizeFunction(function); });
	for (auto &function : functions.functions) {
		table_function.functions.AddFunction(std::move(function));
	}
}

static optional_ptr<CatalogEntry> TryGetEntry(DatabaseInstance &db, const Identifier &name, CatalogType type) {
	D_ASSERT(!name.empty());
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	return schema.GetEntry(data, type, name);
}

optional_ptr<CatalogEntry> ExtensionLoader::TryGetFunction(const Identifier &name) {
	return TryGetEntry(db, name, CatalogType::SCALAR_FUNCTION_ENTRY);
}

ScalarFunctionCatalogEntry &ExtensionLoader::GetFunction(const Identifier &name) {
	auto catalog_entry = TryGetFunction(name);
	if (!catalog_entry) {
		throw InvalidInputException("Function with name %s not found in ExtensionLoader::GetFunction", name);
	}
	return catalog_entry->Cast<ScalarFunctionCatalogEntry>();
}

optional_ptr<CatalogEntry> ExtensionLoader::TryGetTableFunction(const Identifier &name) {
	return TryGetEntry(db, name, CatalogType::TABLE_FUNCTION_ENTRY);
}

TableFunctionCatalogEntry &ExtensionLoader::GetTableFunction(const Identifier &name) {
	auto catalog_entry = TryGetTableFunction(name);
	if (!catalog_entry) {
		throw InvalidInputException("Function with name %s not found in ExtensionLoader::GetTableFunction", name);
	}
	return catalog_entry->Cast<TableFunctionCatalogEntry>();
}

void ExtensionLoader::RegisterType(string type_name, LogicalType type, bind_logical_type_function_t bind_modifiers) {
	D_ASSERT(!type_name.empty());
	CreateTypeInfo info(std::move(type_name), std::move(type), bind_modifiers);
	RegisterType(info);
}

void ExtensionLoader::RegisterType(string type_name, LogicalType type, TypeConstructorSet constructors) {
	D_ASSERT(!type_name.empty());
	CreateTypeInfo info(std::move(type_name), std::move(type), std::move(constructors));
	RegisterType(info);
}

void ExtensionLoader::RegisterType(CreateTypeInfo &info) {
	info.temporary = true;
	info.internal = true;
	info.extension_name = GetRegisteredExtensionName();
	info.SetQualifiedName(
	    QualifiedName(info.GetQualifiedName().Catalog(), loader_info.extension_schema, info.GetQualifiedName().Name()));
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

void ExtensionLoader::RegisterCombineTypesRule(CombineTypesRule rule) {
	DBConfig::GetConfig(db).GetCastFunctions().RegisterCombineTypesRule(rule);
}

void ExtensionLoader::RegisterMetric(MetricInfo info) {
	MetricsManager::Get(db).RegisterMetric(std::move(info));
}

} // namespace duckdb
