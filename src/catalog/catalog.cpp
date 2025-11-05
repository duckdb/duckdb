#include "duckdb/catalog/catalog.hpp"

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/main/extension_helper.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
#include "duckdb/parser/parsed_data/create_secret_info.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/drop_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression_binder/index_binder.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/main/extension_entries.hpp"
#include "duckdb/main/extension/generated_extension_loader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/catalog/similar_catalog_entry.hpp"
#include "duckdb/storage/database_size.hpp"
#include "duckdb/main/settings.hpp"
#include <algorithm>

namespace duckdb {

Catalog::Catalog(AttachedDatabase &db) : db(db) {
}

Catalog::~Catalog() {
}

DatabaseInstance &Catalog::GetDatabase() {
	return db.GetDatabase();
}

AttachedDatabase &Catalog::GetAttached() {
	return db;
}

const AttachedDatabase &Catalog::GetAttached() const {
	return db;
}

const string &Catalog::GetName() const {
	return GetAttached().GetName();
}

idx_t Catalog::GetOid() {
	return GetAttached().oid;
}

Catalog &Catalog::GetSystemCatalog(ClientContext &context) {
	return Catalog::GetSystemCatalog(*context.db);
}

const string &GetDefaultCatalog(CatalogEntryRetriever &retriever) {
	return DatabaseManager::GetDefaultDatabase(retriever.GetContext());
}

optional_ptr<Catalog> Catalog::GetCatalogEntry(CatalogEntryRetriever &retriever, const string &catalog_name) {
	auto &context = retriever.GetContext();
	auto &db_manager = DatabaseManager::Get(context);
	if (catalog_name == TEMP_CATALOG) {
		return &ClientData::Get(context).temporary_objects->GetCatalog();
	}
	if (catalog_name == SYSTEM_CATALOG) {
		return &GetSystemCatalog(context);
	}
	auto entry =
	    db_manager.GetDatabase(context, IsInvalidCatalog(catalog_name) ? GetDefaultCatalog(retriever) : catalog_name);
	if (!entry) {
		return nullptr;
	}
	return &entry->GetCatalog();
}

optional_ptr<Catalog> Catalog::GetCatalogEntry(ClientContext &context, const string &catalog_name) {
	CatalogEntryRetriever entry_retriever(context);
	return GetCatalogEntry(entry_retriever, catalog_name);
}

Catalog &Catalog::GetCatalog(CatalogEntryRetriever &retriever, const string &catalog_name) {
	auto catalog = Catalog::GetCatalogEntry(retriever, catalog_name);
	if (!catalog) {
		throw BinderException("Catalog \"%s\" does not exist!", catalog_name);
	}
	return *catalog;
}

Catalog &Catalog::GetCatalog(ClientContext &context, const string &catalog_name) {
	CatalogEntryRetriever entry_retriever(context);
	return GetCatalog(entry_retriever, catalog_name);
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateSchema(ClientContext &context, CreateSchemaInfo &info) {
	return CreateSchema(GetCatalogTransaction(context), info);
}

CatalogTransaction Catalog::GetCatalogTransaction(ClientContext &context) {
	return CatalogTransaction(*this, context);
}

//===--------------------------------------------------------------------===//
// Table
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateTable(ClientContext &context, BoundCreateTableInfo &info) {
	return CreateTable(GetCatalogTransaction(context), info);
}

optional_ptr<CatalogEntry> Catalog::CreateTable(ClientContext &context, unique_ptr<CreateTableInfo> info) {
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(std::move(info));
	return CreateTable(context, *bound_info);
}

optional_ptr<CatalogEntry> Catalog::CreateTable(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                                                BoundCreateTableInfo &info) {
	return schema.CreateTable(transaction, info);
}

optional_ptr<CatalogEntry> Catalog::CreateTable(CatalogTransaction transaction, BoundCreateTableInfo &info) {
	auto &schema = GetSchema(transaction, info.base->schema);
	return CreateTable(transaction, schema, info);
}

//===--------------------------------------------------------------------===//
// View
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateView(CatalogTransaction transaction, CreateViewInfo &info) {
	auto &schema = GetSchema(transaction, info.schema);
	return CreateView(transaction, schema, info);
}

optional_ptr<CatalogEntry> Catalog::CreateView(ClientContext &context, CreateViewInfo &info) {
	return CreateView(GetCatalogTransaction(context), info);
}

optional_ptr<CatalogEntry> Catalog::CreateView(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                                               CreateViewInfo &info) {
	return schema.CreateView(transaction, info);
}

//===--------------------------------------------------------------------===//
// Sequence
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateSequence(CatalogTransaction transaction, CreateSequenceInfo &info) {
	auto &schema = GetSchema(transaction, info.schema);
	return CreateSequence(transaction, schema, info);
}

optional_ptr<CatalogEntry> Catalog::CreateSequence(ClientContext &context, CreateSequenceInfo &info) {
	return CreateSequence(GetCatalogTransaction(context), info);
}

optional_ptr<CatalogEntry> Catalog::CreateSequence(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                                                   CreateSequenceInfo &info) {
	return schema.CreateSequence(transaction, info);
}

//===--------------------------------------------------------------------===//
// Type
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateType(CatalogTransaction transaction, CreateTypeInfo &info) {
	auto &schema = GetSchema(transaction, info.schema);
	return CreateType(transaction, schema, info);
}

optional_ptr<CatalogEntry> Catalog::CreateType(ClientContext &context, CreateTypeInfo &info) {
	return CreateType(GetCatalogTransaction(context), info);
}

optional_ptr<CatalogEntry> Catalog::CreateType(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                                               CreateTypeInfo &info) {
	return schema.CreateType(transaction, info);
}

//===--------------------------------------------------------------------===//
// Table Function
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateTableFunction(CatalogTransaction transaction, CreateTableFunctionInfo &info) {
	auto &schema = GetSchema(transaction, info.schema);
	return CreateTableFunction(transaction, schema, info);
}

optional_ptr<CatalogEntry> Catalog::CreateTableFunction(ClientContext &context, CreateTableFunctionInfo &info) {
	return CreateTableFunction(GetCatalogTransaction(context), info);
}

optional_ptr<CatalogEntry> Catalog::CreateTableFunction(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                                                        CreateTableFunctionInfo &info) {
	return schema.CreateTableFunction(transaction, info);
}

optional_ptr<CatalogEntry> Catalog::CreateTableFunction(ClientContext &context,
                                                        optional_ptr<CreateTableFunctionInfo> info) {
	return CreateTableFunction(context, *info);
}

//===--------------------------------------------------------------------===//
// Copy Function
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateCopyFunction(CatalogTransaction transaction, CreateCopyFunctionInfo &info) {
	auto &schema = GetSchema(transaction, info.schema);
	return CreateCopyFunction(transaction, schema, info);
}

optional_ptr<CatalogEntry> Catalog::CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo &info) {
	return CreateCopyFunction(GetCatalogTransaction(context), info);
}

optional_ptr<CatalogEntry> Catalog::CreateCopyFunction(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                                                       CreateCopyFunctionInfo &info) {
	return schema.CreateCopyFunction(transaction, info);
}

//===--------------------------------------------------------------------===//
// Pragma Function
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreatePragmaFunction(CatalogTransaction transaction,
                                                         CreatePragmaFunctionInfo &info) {
	auto &schema = GetSchema(transaction, info.schema);
	return CreatePragmaFunction(transaction, schema, info);
}

optional_ptr<CatalogEntry> Catalog::CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo &info) {
	return CreatePragmaFunction(GetCatalogTransaction(context), info);
}

optional_ptr<CatalogEntry> Catalog::CreatePragmaFunction(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                                                         CreatePragmaFunctionInfo &info) {
	return schema.CreatePragmaFunction(transaction, info);
}

//===--------------------------------------------------------------------===//
// Function
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateFunction(CatalogTransaction transaction, CreateFunctionInfo &info) {
	auto &schema = GetSchema(transaction, info.schema);
	return CreateFunction(transaction, schema, info);
}

optional_ptr<CatalogEntry> Catalog::CreateFunction(ClientContext &context, CreateFunctionInfo &info) {
	return CreateFunction(GetCatalogTransaction(context), info);
}

optional_ptr<CatalogEntry> Catalog::CreateFunction(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                                                   CreateFunctionInfo &info) {
	return schema.CreateFunction(transaction, info);
}

optional_ptr<CatalogEntry> Catalog::AddFunction(ClientContext &context, CreateFunctionInfo &info) {
	info.on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	return CreateFunction(context, info);
}

//===--------------------------------------------------------------------===//
// Collation
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateCollation(CatalogTransaction transaction, CreateCollationInfo &info) {
	auto &schema = GetSchema(transaction, info.schema);
	return CreateCollation(transaction, schema, info);
}

optional_ptr<CatalogEntry> Catalog::CreateCollation(ClientContext &context, CreateCollationInfo &info) {
	return CreateCollation(GetCatalogTransaction(context), info);
}

optional_ptr<CatalogEntry> Catalog::CreateCollation(CatalogTransaction transaction, SchemaCatalogEntry &schema,
                                                    CreateCollationInfo &info) {
	return schema.CreateCollation(transaction, info);
}

//===--------------------------------------------------------------------===//
// Index
//===--------------------------------------------------------------------===//
optional_ptr<CatalogEntry> Catalog::CreateIndex(CatalogTransaction transaction, CreateIndexInfo &info) {
	auto &schema = GetSchema(transaction, info.schema);
	auto &table = schema.GetEntry(transaction, CatalogType::TABLE_ENTRY, info.table)->Cast<TableCatalogEntry>();
	return schema.CreateIndex(transaction, info, table);
}

optional_ptr<CatalogEntry> Catalog::CreateIndex(ClientContext &context, CreateIndexInfo &info) {
	return CreateIndex(GetCatalogTransaction(context), info);
}

unique_ptr<LogicalOperator> Catalog::BindCreateIndex(Binder &binder, CreateStatement &stmt, TableCatalogEntry &table,
                                                     unique_ptr<LogicalOperator> plan) {
	D_ASSERT(plan->type == LogicalOperatorType::LOGICAL_GET);
	auto create_index_info = unique_ptr_cast<CreateInfo, CreateIndexInfo>(std::move(stmt.info));
	IndexBinder index_binder(binder, binder.context);
	return index_binder.BindCreateIndex(binder.context, std::move(create_index_info), table, std::move(plan), nullptr);
}

unique_ptr<LogicalOperator> Catalog::BindAlterAddIndex(Binder &binder, TableCatalogEntry &table_entry,
                                                       unique_ptr<LogicalOperator> plan,
                                                       unique_ptr<CreateIndexInfo> create_info,
                                                       unique_ptr<AlterTableInfo> alter_info) {
	throw NotImplementedException("BindAlterAddIndex not supported by this catalog");
}

//===--------------------------------------------------------------------===//
// Lookup Structures
//===--------------------------------------------------------------------===//
struct CatalogLookup {
	CatalogLookup(Catalog &catalog, CatalogType catalog_type, string schema_p, string name_p)
	    : catalog(catalog), schema(std::move(schema_p)), name(std::move(name_p)), lookup_info(catalog_type, name) {
	}
	CatalogLookup(Catalog &catalog, string schema_p, const EntryLookupInfo &lookup_p)
	    : catalog(catalog), schema(std::move(schema_p)), name(lookup_p.GetEntryName()), lookup_info(lookup_p, name) {
	}

	Catalog &catalog;
	string schema;
	string name;
	EntryLookupInfo lookup_info;
};

//===--------------------------------------------------------------------===//
// Generic
//===--------------------------------------------------------------------===//
void Catalog::DropEntry(ClientContext &context, DropInfo &info) {
	if (info.type == CatalogType::SCHEMA_ENTRY) {
		// DROP SCHEMA
		DropSchema(context, info);
		return;
	}

	CatalogEntryRetriever retriever(context);
	EntryLookupInfo lookup_info(info.type, info.name);
	auto lookup = LookupEntry(retriever, info.schema, lookup_info, info.if_not_found);
	if (!lookup.Found()) {
		return;
	}

	lookup.schema->DropEntry(context, info);
}

SchemaCatalogEntry &Catalog::GetSchema(ClientContext &context, const string &schema) {
	return GetSchema(GetCatalogTransaction(context), schema);
}

SchemaCatalogEntry &Catalog::GetSchema(CatalogTransaction transaction, const string &schema) {
	EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, schema);
	return GetSchema(transaction, schema_lookup);
}

SchemaCatalogEntry &Catalog::GetSchema(ClientContext &context, const EntryLookupInfo &schema_lookup) {
	return *Catalog::GetSchema(context, schema_lookup, OnEntryNotFound::THROW_EXCEPTION);
}

SchemaCatalogEntry &Catalog::GetSchema(ClientContext &context, const string &catalog_name, const string &schema) {
	return *GetSchema(context, catalog_name, schema, OnEntryNotFound::THROW_EXCEPTION);
}

optional_ptr<SchemaCatalogEntry> Catalog::GetSchema(ClientContext &context, const string &schema,
                                                    OnEntryNotFound if_not_found) {
	return GetSchema(GetCatalogTransaction(context), schema, if_not_found);
}

optional_ptr<SchemaCatalogEntry> Catalog::GetSchema(CatalogTransaction transaction, const string &schema,
                                                    OnEntryNotFound if_not_found) {
	EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, schema);
	return LookupSchema(transaction, schema_lookup, if_not_found);
}

optional_ptr<SchemaCatalogEntry> Catalog::GetSchema(ClientContext &context, const EntryLookupInfo &schema_lookup,
                                                    OnEntryNotFound if_not_found) {
	return LookupSchema(GetCatalogTransaction(context), schema_lookup, if_not_found);
}

optional_ptr<SchemaCatalogEntry> Catalog::GetSchema(ClientContext &context, const string &catalog_name,
                                                    const string &schema, OnEntryNotFound if_not_found) {
	EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, schema);
	return GetSchema(context, catalog_name, schema_lookup, if_not_found);
}

SchemaCatalogEntry &Catalog::GetSchema(ClientContext &context, const string &catalog_name,
                                       const EntryLookupInfo &schema_lookup) {
	return *Catalog::GetSchema(context, catalog_name, schema_lookup, OnEntryNotFound::THROW_EXCEPTION);
}

SchemaCatalogEntry &Catalog::GetSchema(CatalogTransaction transaction, const EntryLookupInfo &schema_lookup) {
	return *LookupSchema(transaction, schema_lookup, OnEntryNotFound::THROW_EXCEPTION);
}

bool Catalog::CheckAmbiguousCatalogOrSchema(ClientContext &context, const string &schema) {
	EntryLookupInfo schema_lookup(CatalogType::SCHEMA_ENTRY, schema);
	return !!GetSchema(context, schema_lookup, OnEntryNotFound::RETURN_NULL);
}

//===--------------------------------------------------------------------===//
// Lookup
//===--------------------------------------------------------------------===//
vector<SimilarCatalogEntry> Catalog::SimilarEntriesInSchemas(ClientContext &context, const EntryLookupInfo &lookup_info,
                                                             const reference_set_t<SchemaCatalogEntry> &schemas) {
	vector<SimilarCatalogEntry> results;
	for (auto schema_ref : schemas) {
		auto &schema = schema_ref.get();
		auto transaction = schema.catalog.GetCatalogTransaction(context);
		auto entry = schema.GetSimilarEntry(transaction, lookup_info);
		if (!entry.Found()) {
			// no similar entry found
			continue;
		}
		if (results.empty() || results[0].score <= entry.score) {
			if (!results.empty() && results[0].score < entry.score) {
				results.clear();
			}

			results.push_back(entry);
			results.back().schema = &schema;
		}
	}
	return results;
}

vector<CatalogSearchEntry> GetCatalogEntries(CatalogEntryRetriever &retriever, const string &catalog,
                                             const string &schema) {
	auto &context = retriever.GetContext();
	vector<CatalogSearchEntry> entries;
	auto &search_path = retriever.GetSearchPath();
	if (IsInvalidCatalog(catalog) && IsInvalidSchema(schema)) {
		// no catalog or schema provided - scan the entire search path
		entries = search_path.Get();
	} else if (IsInvalidCatalog(catalog)) {
		auto catalogs = search_path.GetCatalogsForSchema(schema);
		for (auto &catalog_name : catalogs) {
			entries.emplace_back(catalog_name, schema);
		}
		if (entries.empty()) {
			auto &default_entry = search_path.GetDefault();
			if (!IsInvalidCatalog(default_entry.catalog)) {
				entries.emplace_back(default_entry.catalog, schema);
			} else {
				entries.emplace_back(DatabaseManager::GetDefaultDatabase(context), schema);
			}
		}
	} else if (IsInvalidSchema(schema)) {
		auto schemas = search_path.GetSchemasForCatalog(catalog);
		for (auto &schema_name : schemas) {
			entries.emplace_back(catalog, schema_name);
		}
		if (entries.empty()) {
			auto catalog_entry = Catalog::GetCatalogEntry(context, catalog);
			if (catalog_entry) {
				entries.emplace_back(catalog, catalog_entry->GetDefaultSchema());
			} else {
				entries.emplace_back(catalog, DEFAULT_SCHEMA);
			}
		}
	} else {
		// specific catalog and schema provided
		entries.emplace_back(catalog, schema);
	}
	return entries;
}

void FindMinimalQualification(CatalogEntryRetriever &retriever, const string &catalog_name, const string &schema_name,
                              bool &qualify_database, bool &qualify_schema) {
	// check if we can we qualify ONLY the schema
	bool found = false;
	auto entries = GetCatalogEntries(retriever, INVALID_CATALOG, schema_name);
	for (auto &entry : entries) {
		if (entry.catalog == catalog_name && entry.schema == schema_name) {
			found = true;
			break;
		}
	}
	if (found) {
		qualify_database = false;
		qualify_schema = true;
		return;
	}
	// check if we can qualify ONLY the catalog
	found = false;
	entries = GetCatalogEntries(retriever, catalog_name, INVALID_SCHEMA);
	for (auto &entry : entries) {
		if (entry.catalog == catalog_name && entry.schema == schema_name) {
			found = true;
			break;
		}
	}
	if (found) {
		qualify_database = true;
		qualify_schema = false;
		return;
	}
	// need to qualify both catalog and schema
	qualify_database = true;
	qualify_schema = true;
}

bool Catalog::TryAutoLoad(ClientContext &context, const string &original_name) noexcept {
	string extension_name = ExtensionHelper::ApplyExtensionAlias(original_name);
	if (context.db->ExtensionIsLoaded(extension_name)) {
		return true;
	}
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
	auto &dbconfig = DBConfig::GetConfig(context);
	if (!dbconfig.options.autoload_known_extensions) {
		return false;
	}
	try {
		if (ExtensionHelper::CanAutoloadExtension(extension_name)) {
			return ExtensionHelper::TryAutoLoadExtension(context, extension_name);
		}
	} catch (...) {
		return false;
	}
#endif
	return false;
}

String Catalog::AutoloadExtensionByConfigName(ClientContext &context, const String &configuration_name) {
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
	auto &dbconfig = DBConfig::GetConfig(context);
	if (dbconfig.options.autoload_known_extensions) {
		auto extension_name =
		    ExtensionHelper::FindExtensionInEntries(configuration_name.ToStdString(), EXTENSION_SETTINGS);
		if (ExtensionHelper::CanAutoloadExtension(extension_name)) {
			ExtensionHelper::AutoLoadExtension(context, extension_name);
			return extension_name;
		}
	}
#endif

	throw Catalog::UnrecognizedConfigurationError(context, configuration_name.ToStdString());
}

static bool IsAutoloadableFunction(CatalogType type) {
	return (type == CatalogType::TABLE_FUNCTION_ENTRY || type == CatalogType::SCALAR_FUNCTION_ENTRY ||
	        type == CatalogType::AGGREGATE_FUNCTION_ENTRY || type == CatalogType::PRAGMA_FUNCTION_ENTRY);
}

bool IsTableFunction(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_FUNCTION_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
	case CatalogType::PRAGMA_FUNCTION_ENTRY:
		return true;
	default:
		return false;
	}
}

bool IsScalarFunction(CatalogType type) {
	switch (type) {
	case CatalogType::SCALAR_FUNCTION_ENTRY:
	case CatalogType::AGGREGATE_FUNCTION_ENTRY:
	case CatalogType::MACRO_ENTRY:
		return true;
	default:
		return false;
	}
}

static bool CompareCatalogTypes(CatalogType type_a, CatalogType type_b) {
	if (type_a == type_b) {
		// Types are same
		return true;
	}
	if (IsScalarFunction(type_a) && IsScalarFunction(type_b)) {
		return true;
	}
	if (IsTableFunction(type_a) && IsTableFunction(type_b)) {
		return true;
	}
	return false;
}

bool Catalog::AutoLoadExtensionByCatalogEntry(DatabaseInstance &db, CatalogType type, const string &entry_name) {
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
	auto &dbconfig = DBConfig::GetConfig(db);
	if (dbconfig.options.autoload_known_extensions) {
		string extension_name;
		if (IsAutoloadableFunction(type)) {
			auto lookup_result = ExtensionHelper::FindExtensionInFunctionEntries(entry_name, EXTENSION_FUNCTIONS);
			if (lookup_result.empty()) {
				return false;
			}
			for (auto &function : lookup_result) {
				auto function_type = function.second;
				// FIXME: what if there are two functions with the same name, from different extensions?
				if (CompareCatalogTypes(type, function_type)) {
					extension_name = function.first;
					break;
				}
			}
		} else if (type == CatalogType::COPY_FUNCTION_ENTRY) {
			extension_name = ExtensionHelper::FindExtensionInEntries(entry_name, EXTENSION_COPY_FUNCTIONS);
		} else if (type == CatalogType::TYPE_ENTRY) {
			extension_name = ExtensionHelper::FindExtensionInEntries(entry_name, EXTENSION_TYPES);
		} else if (type == CatalogType::COLLATION_ENTRY) {
			extension_name = ExtensionHelper::FindExtensionInEntries(entry_name, EXTENSION_COLLATIONS);
		}

		if (!extension_name.empty() && ExtensionHelper::CanAutoloadExtension(extension_name)) {
			ExtensionHelper::AutoLoadExtension(db, extension_name);
			return true;
		}
	}
#endif

	return false;
}

CatalogException Catalog::UnrecognizedConfigurationError(ClientContext &context, const string &name) {
	// check if the setting exists in any extensions
	auto extension_name = ExtensionHelper::FindExtensionInEntries(name, EXTENSION_SETTINGS);
	if (!extension_name.empty()) {
		auto error_message = "Setting with name \"" + name + "\" is not in the catalog, but it exists in the " +
		                     extension_name + " extension.";
		error_message = ExtensionHelper::AddExtensionInstallHintToErrorMsg(context, error_message, extension_name);
		return CatalogException(error_message);
	}
	// the setting is not in an extension
	// get a list of all options
	vector<string> potential_names = DBConfig::GetOptionNames();
	for (auto &entry : DBConfig::GetConfig(context).extension_parameters) {
		potential_names.push_back(entry.first);
	}
	throw CatalogException::MissingEntry("configuration parameter", name, potential_names);
}

CatalogException Catalog::CreateMissingEntryException(CatalogEntryRetriever &retriever,
                                                      const EntryLookupInfo &lookup_info,
                                                      const reference_set_t<SchemaCatalogEntry> &schemas) {
	auto &context = retriever.GetContext();
	auto entries = SimilarEntriesInSchemas(context, lookup_info, schemas);
	auto max_schema_count = DBConfig::GetSetting<CatalogErrorMaxSchemasSetting>(context);

	reference_set_t<SchemaCatalogEntry> unseen_schemas;
	auto &db_manager = DatabaseManager::Get(context);
	auto databases = db_manager.GetDatabases(context, max_schema_count);

	for (const auto &database : databases) {
		if (unseen_schemas.size() >= max_schema_count) {
			break;
		}
		auto &catalog = database->GetCatalog();
		auto current_schemas = catalog.GetSchemas(context);
		for (auto &current_schema : current_schemas) {
			if (unseen_schemas.size() >= max_schema_count) {
				break;
			}
			unseen_schemas.insert(current_schema.get());
		}
	}

	// Check if the entry exists in any extension.
	string extension_name;
	auto type = lookup_info.GetCatalogType();
	auto &entry_name = lookup_info.GetEntryName();
	if (type == CatalogType::TABLE_FUNCTION_ENTRY || type == CatalogType::SCALAR_FUNCTION_ENTRY ||
	    type == CatalogType::AGGREGATE_FUNCTION_ENTRY || type == CatalogType::PRAGMA_FUNCTION_ENTRY) {
		auto lookup_result = ExtensionHelper::FindExtensionInFunctionEntries(entry_name, EXTENSION_FUNCTIONS);
		do {
			if (lookup_result.empty()) {
				break;
			}
			vector<string> other_types;
			string extension_for_error;
			for (auto &function : lookup_result) {
				auto function_type = function.second;
				if (CompareCatalogTypes(type, function_type)) {
					extension_name = function.first;
					break;
				}
				extension_for_error = function.first;
				other_types.push_back(CatalogTypeToString(function_type));
			}
			if (!extension_name.empty()) {
				break;
			}
			if (other_types.size() == 1) {
				auto &function_type = other_types[0];
				auto error =
				    CatalogException("%s with name \"%s\" is not in the catalog, a function by this name exists "
				                     "in the %s extension, but it's of a different type, namely %s",
				                     CatalogTypeToString(type), entry_name, extension_for_error, function_type);
				return error;
			} else {
				D_ASSERT(!other_types.empty());
				auto list_of_types = StringUtil::Join(other_types, ", ");
				auto error =
				    CatalogException("%s with name \"%s\" is not in the catalog, functions with this name exist "
				                     "in the %s extension, but they are of different types, namely %s",
				                     CatalogTypeToString(type), entry_name, extension_for_error, list_of_types);
				return error;
			}
		} while (false);
	} else if (type == CatalogType::TYPE_ENTRY) {
		extension_name = ExtensionHelper::FindExtensionInEntries(entry_name, EXTENSION_TYPES);
	} else if (type == CatalogType::COPY_FUNCTION_ENTRY) {
		extension_name = ExtensionHelper::FindExtensionInEntries(entry_name, EXTENSION_COPY_FUNCTIONS);
	} else if (type == CatalogType::COLLATION_ENTRY) {
		extension_name = ExtensionHelper::FindExtensionInEntries(entry_name, EXTENSION_COLLATIONS);
	}

	// if we found an extension that can handle this catalog entry, create an error hinting the user
	if (!extension_name.empty()) {
		auto error_message = CatalogTypeToString(type) + " with name \"" + entry_name +
		                     "\" is not in the catalog, but it exists in the " + extension_name + " extension.";
		error_message = ExtensionHelper::AddExtensionInstallHintToErrorMsg(context, error_message, extension_name);
		return CatalogException(error_message);
	}

	// entries in other schemas get a penalty
	// however, if there is an exact match in another schema, we will always show it
	static constexpr const double UNSEEN_PENALTY = 0.2;
	auto unseen_entries = SimilarEntriesInSchemas(context, lookup_info, unseen_schemas);
	set<string> suggestions;

	if (!unseen_entries.empty() && (unseen_entries[0].score == 1.0 || unseen_entries[0].score - UNSEEN_PENALTY >
	                                                                      (entries.empty() ? 0.0 : entries[0].score))) {
		// the closest matching entry requires qualification as it is not in the default search path
		// check how to minimally qualify this entry
		for (auto &unseen_entry : unseen_entries) {
			auto catalog_name = unseen_entry.schema->catalog.GetName();
			auto schema_name = unseen_entry.schema->name;
			bool qualify_database;
			bool qualify_schema;
			FindMinimalQualification(retriever, catalog_name, schema_name, qualify_database, qualify_schema);
			auto qualified_name = unseen_entry.GetQualifiedName(qualify_database, qualify_schema);
			suggestions.insert(qualified_name);
		}
	} else if (!entries.empty()) {
		for (auto &entry : entries) {
			suggestions.insert(entry.name);
		}
	}

	string did_you_mean;
	if (suggestions.size() > 2) {
		string last = *suggestions.rbegin();
		suggestions.erase(last);
		did_you_mean = StringUtil::Join(suggestions, ", ") + ", or " + last;
	} else {
		did_you_mean = StringUtil::Join(suggestions, " or ");
	}

	return CatalogException::MissingEntry(lookup_info, did_you_mean);
}

CatalogEntryLookup Catalog::TryLookupEntryInternal(CatalogTransaction transaction, const string &schema,
                                                   const EntryLookupInfo &lookup_info) {
	if (lookup_info.GetAtClause() && !SupportsTimeTravel()) {
		return {nullptr, nullptr, ErrorData(BinderException("Catalog type does not support time travel"))};
	}
	auto schema_lookup = EntryLookupInfo::SchemaLookup(lookup_info, schema);
	auto schema_entry = LookupSchema(transaction, schema_lookup, OnEntryNotFound::RETURN_NULL);
	if (!schema_entry) {
		return {nullptr, nullptr, ErrorData()};
	}
	auto entry = schema_entry->LookupEntry(transaction, lookup_info);
	if (!entry) {
		return {schema_entry, nullptr, ErrorData()};
	}
	return {schema_entry, entry, ErrorData()};
}

CatalogEntryLookup Catalog::TryLookupEntry(CatalogEntryRetriever &retriever, const string &schema,
                                           const EntryLookupInfo &lookup_info, OnEntryNotFound if_not_found) {
	auto &context = retriever.GetContext();
	reference_set_t<SchemaCatalogEntry> schemas;
	if (IsInvalidSchema(schema)) {
		// try all schemas for this catalog
		auto entries = GetCatalogEntries(retriever, GetName(), INVALID_SCHEMA);
		for (auto &entry : entries) {
			auto &candidate_schema = entry.schema;
			auto transaction = GetCatalogTransaction(context);
			auto result = TryLookupEntryInternal(transaction, candidate_schema, lookup_info);
			if (result.Found()) {
				return result;
			}
			if (result.schema) {
				schemas.insert(*result.schema);
			}
		}
	} else {
		auto transaction = GetCatalogTransaction(context);
		auto result = TryLookupEntryInternal(transaction, schema, lookup_info);
		if (result.Found()) {
			return result;
		}
		if (result.schema) {
			schemas.insert(*result.schema);
		}
	}

	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return {nullptr, nullptr, ErrorData()};
	}
	// Check if the default database is actually attached. CreateMissingEntryException will throw binder exception
	// otherwise.
	if (!GetCatalogEntry(context, GetDefaultCatalog(retriever))) {
		auto except = CatalogException("%s with name %s does not exist!",
		                               CatalogTypeToString(lookup_info.GetCatalogType()), lookup_info.GetEntryName());
		return {nullptr, nullptr, ErrorData(except)};
	} else {
		auto except = CreateMissingEntryException(retriever, lookup_info, schemas);
		return {nullptr, nullptr, ErrorData(except)};
	}
}

CatalogEntryLookup Catalog::LookupEntry(CatalogEntryRetriever &retriever, const string &schema,
                                        const EntryLookupInfo &lookup_info, OnEntryNotFound if_not_found) {
	auto res = TryLookupEntry(retriever, schema, lookup_info, if_not_found);

	if (res.error.HasError()) {
		res.error.Throw();
	}

	return res;
}

static void ThrowDefaultTableAmbiguityException(CatalogEntryLookup &base_lookup, CatalogEntryLookup &default_table,
                                                const string &name) {
	auto entry_type = CatalogTypeToString(base_lookup.entry->type);
	string fully_qualified_name_hint;
	if (base_lookup.schema) {
		fully_qualified_name_hint = StringUtil::Format(": '%s.%s.%s'", base_lookup.schema->catalog.GetName(),
		                                               base_lookup.schema->name, base_lookup.entry->name);
	}
	string fully_qualified_catalog_name_hint = StringUtil::Format(
	    ": '%s.%s.%s'", default_table.schema->catalog.GetName(), default_table.schema->name, default_table.entry->name);
	throw CatalogException(
	    "Ambiguity detected for '%s': this could either refer to the '%s' '%s', or the "
	    "attached catalog '%s' which has a default table. To avoid this error, either detach the catalog and "
	    "reattach under a different name, or use a fully qualified name for the '%s'%s or for the Catalog "
	    "Default Table%s.",
	    name, entry_type, name, name, entry_type, fully_qualified_name_hint, fully_qualified_catalog_name_hint);
}

CatalogEntryLookup Catalog::TryLookupEntry(CatalogEntryRetriever &retriever, const vector<CatalogLookup> &lookups,
                                           const EntryLookupInfo &lookup_info, OnEntryNotFound if_not_found,
                                           bool allow_default_table_lookup) {
	auto &context = retriever.GetContext();
	reference_set_t<SchemaCatalogEntry> schemas;
	bool all_errors = true;
	ErrorData error_data;
	CatalogEntryLookup result;
	for (auto &lookup : lookups) {
		auto transaction = lookup.catalog.GetCatalogTransaction(context);
		result = lookup.catalog.TryLookupEntryInternal(transaction, lookup.schema, lookup.lookup_info);
		if (result.Found()) {
			break;
		}
		if (result.schema) {
			schemas.insert(*result.schema);
		}
		if (!result.error.HasError()) {
			all_errors = false;
		} else {
			error_data = std::move(result.error);
		}
	}

	// Special case for tables: we do a second lookup searching for catalogs with default tables that also match this
	// lookup
	if (lookup_info.GetCatalogType() == CatalogType::TABLE_ENTRY && allow_default_table_lookup) {
		if (!result.Found()) {
			result = TryLookupDefaultTable(retriever, lookup_info, false);
			if (result.error.HasError()) {
				error_data = std::move(result.error);
			}
		} else {
			// allow_ignore_at_clause set to true to ensure `FROM <table_name> AT <version>` is considered
			// ambiguous with a default table lookup in a catalog that does not support time travel
			auto ambiguity_lookup = TryLookupDefaultTable(retriever, lookup_info, true);
			if (ambiguity_lookup.Found()) {
				ThrowDefaultTableAmbiguityException(result, ambiguity_lookup, lookup_info.GetEntryName());
			}
		}
	}

	if (result.Found()) {
		return result;
	}

	if (all_errors && error_data.HasError()) {
		error_data.Throw();
	}

	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return {nullptr, nullptr, ErrorData()};
	}
	// Check if the default database is actually attached. CreateMissingEntryException will throw binder exception
	// otherwise.
	if (!GetCatalogEntry(context, GetDefaultCatalog(retriever))) {
		auto except = CatalogException("%s with name %s does not exist!",
		                               CatalogTypeToString(lookup_info.GetCatalogType()), lookup_info.GetEntryName());
		return {nullptr, nullptr, ErrorData(except)};
	} else {
		auto except = CreateMissingEntryException(retriever, lookup_info, schemas);
		return {nullptr, nullptr, ErrorData(except)};
	}
}

CatalogEntryLookup Catalog::TryLookupDefaultTable(CatalogEntryRetriever &retriever, const EntryLookupInfo &lookup_info,
                                                  bool allow_ignore_at_clause) {
	auto catalog_by_name = GetCatalogEntry(retriever, lookup_info.GetEntryName());

	if (catalog_by_name && catalog_by_name->HasDefaultTable()) {
		auto transaction = catalog_by_name->GetCatalogTransaction(retriever.GetContext());
		QueryErrorContext context;

		string table_schema = catalog_by_name->GetDefaultTableSchema();
		string table_name = catalog_by_name->GetDefaultTable();

		optional_ptr<BoundAtClause> at_clause;
		if (!catalog_by_name->SupportsTimeTravel() && allow_ignore_at_clause) {
			at_clause = nullptr;
		} else {
			at_clause = lookup_info.GetAtClause();
		}

		EntryLookupInfo info = EntryLookupInfo(CatalogType::TABLE_ENTRY, table_name, at_clause, context);
		return catalog_by_name->TryLookupEntryInternal(transaction, table_schema, info);
	}

	return {nullptr, nullptr, ErrorData()};
}

CatalogEntryLookup Catalog::TryLookupEntry(CatalogEntryRetriever &retriever, const string &catalog,
                                           const string &schema, const EntryLookupInfo &lookup_info,
                                           OnEntryNotFound if_not_found) {
	auto entries = GetCatalogEntries(retriever, catalog, schema);
	vector<CatalogLookup> lookups;
	vector<CatalogLookup> final_lookups;
	lookups.reserve(entries.size());
	for (auto &entry : entries) {
		optional_ptr<Catalog> catalog_entry;
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			catalog_entry = Catalog::GetCatalogEntry(retriever, entry.catalog);
		} else {
			catalog_entry = &Catalog::GetCatalog(retriever, entry.catalog);
		}
		if (!catalog_entry) {
			return {nullptr, nullptr, ErrorData()};
		}
		D_ASSERT(catalog_entry);
		auto lookup_behavior = catalog_entry->CatalogTypeLookupRule(lookup_info.GetCatalogType());
		if (lookup_behavior == CatalogLookupBehavior::STANDARD) {
			lookups.emplace_back(*catalog_entry, entry.schema, lookup_info);
		} else if (lookup_behavior == CatalogLookupBehavior::LOWER_PRIORITY) {
			final_lookups.emplace_back(*catalog_entry, entry.schema, lookup_info);
		}
	}

	for (auto &lookup : final_lookups) {
		lookups.emplace_back(std::move(lookup));
	}

	bool allow_default_table_lookup = catalog.empty() && schema.empty();

	return TryLookupEntry(retriever, lookups, lookup_info, if_not_found, allow_default_table_lookup);
}

CatalogEntry &Catalog::GetEntry(ClientContext &context, CatalogType catalog_type, const string &catalog_name,
                                const string &schema_name, const string &name) {
	EntryLookupInfo lookup_info(catalog_type, name);
	return GetEntry(context, catalog_name, schema_name, lookup_info);
}

optional_ptr<CatalogEntry> Catalog::GetEntry(ClientContext &context, CatalogType catalog_type, const string &schema,
                                             const string &name, OnEntryNotFound if_not_found) {
	EntryLookupInfo lookup_info(catalog_type, name);
	return GetEntry(context, schema, lookup_info, if_not_found);
}

CatalogEntry &Catalog::GetEntry(ClientContext &context, CatalogType catalog_type, const string &schema_name,
                                const string &name) {
	EntryLookupInfo lookup_info(catalog_type, name);
	return GetEntry(context, schema_name, lookup_info);
}

optional_ptr<CatalogEntry> Catalog::GetEntry(CatalogEntryRetriever &retriever, const string &schema_name,
                                             const EntryLookupInfo &lookup_info, OnEntryNotFound if_not_found) {
	auto lookup_entry = TryLookupEntry(retriever, schema_name, lookup_info, if_not_found);

	// Try autoloading extension to resolve lookup
	if (!lookup_entry.Found()) {
		if (AutoLoadExtensionByCatalogEntry(*retriever.GetContext().db, lookup_info.GetCatalogType(),
		                                    lookup_info.GetEntryName())) {
			lookup_entry = TryLookupEntry(retriever, schema_name, lookup_info, if_not_found);
		}
	}

	if (lookup_entry.error.HasError()) {
		lookup_entry.error.Throw();
	}

	return lookup_entry.entry.get();
}

optional_ptr<CatalogEntry> Catalog::GetEntry(ClientContext &context, const string &schema_name,
                                             const EntryLookupInfo &lookup_info, OnEntryNotFound if_not_found) {
	CatalogEntryRetriever retriever(context);
	return GetEntry(retriever, schema_name, lookup_info, if_not_found);
}

CatalogEntry &Catalog::GetEntry(ClientContext &context, const string &schema, const EntryLookupInfo &lookup_info) {
	return *Catalog::GetEntry(context, schema, lookup_info, OnEntryNotFound::THROW_EXCEPTION);
}

optional_ptr<CatalogEntry> Catalog::GetEntry(CatalogEntryRetriever &retriever, const string &catalog,
                                             const string &schema, const EntryLookupInfo &lookup_info,
                                             OnEntryNotFound if_not_found) {
	auto result = TryLookupEntry(retriever, catalog, schema, lookup_info, if_not_found);

	// Try autoloading extension to resolve lookup
	if (!result.Found()) {
		if (AutoLoadExtensionByCatalogEntry(*retriever.GetContext().db, lookup_info.GetCatalogType(),
		                                    lookup_info.GetEntryName())) {
			result = TryLookupEntry(retriever, catalog, schema, lookup_info, if_not_found);
		}
	}

	if (result.error.HasError()) {
		result.error.Throw();
	}

	if (!result.Found()) {
		D_ASSERT(if_not_found == OnEntryNotFound::RETURN_NULL);
		return nullptr;
	}
	return result.entry.get();
}
optional_ptr<CatalogEntry> Catalog::GetEntry(ClientContext &context, const string &catalog, const string &schema,
                                             const EntryLookupInfo &lookup_info, OnEntryNotFound if_not_found) {
	CatalogEntryRetriever retriever(context);
	return GetEntry(retriever, catalog, schema, lookup_info, if_not_found);
}

CatalogEntry &Catalog::GetEntry(ClientContext &context, const string &catalog, const string &schema,
                                const EntryLookupInfo &lookup_info) {
	return *Catalog::GetEntry(context, catalog, schema, lookup_info, OnEntryNotFound::THROW_EXCEPTION);
}

optional_ptr<SchemaCatalogEntry> Catalog::GetSchema(CatalogEntryRetriever &retriever, const string &catalog_name,
                                                    const EntryLookupInfo &schema_lookup,
                                                    OnEntryNotFound if_not_found) {
	auto entries = GetCatalogEntries(retriever, catalog_name, schema_lookup.GetEntryName());
	for (idx_t i = 0; i < entries.size(); i++) {
		auto catalog = Catalog::GetCatalogEntry(retriever, entries[i].catalog);
		if (!catalog) {
			// skip if it is not an attached database
			continue;
		}
		const auto on_not_found = i + 1 == entries.size() ? if_not_found : OnEntryNotFound::RETURN_NULL;
		auto result = catalog->GetSchema(retriever.GetContext(), schema_lookup, on_not_found);
		if (result) {
			return result;
		}
	}
	// Catalog has not been found.
	if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException(schema_lookup.GetErrorContext(), "Catalog with name %s does not exist!", catalog_name);
	}
	return nullptr;
}

optional_ptr<SchemaCatalogEntry> Catalog::GetSchema(ClientContext &context, const string &catalog_name,
                                                    const EntryLookupInfo &schema_lookup,
                                                    OnEntryNotFound if_not_found) {
	CatalogEntryRetriever retriever(context);
	return GetSchema(retriever, catalog_name, schema_lookup, if_not_found);
}

vector<reference<SchemaCatalogEntry>> Catalog::GetSchemas(ClientContext &context) {
	vector<reference<SchemaCatalogEntry>> schemas;
	ScanSchemas(context, [&](SchemaCatalogEntry &entry) { schemas.push_back(entry); });
	return schemas;
}

vector<reference<SchemaCatalogEntry>> Catalog::GetSchemas(CatalogEntryRetriever &retriever,
                                                          const string &catalog_name) {
	vector<reference<Catalog>> catalogs;
	if (IsInvalidCatalog(catalog_name)) {
		reference_set_t<Catalog> inserted_catalogs;

		auto &search_path = retriever.GetSearchPath();
		for (auto &entry : search_path.Get()) {
			auto &catalog = Catalog::GetCatalog(retriever, entry.catalog);
			if (inserted_catalogs.find(catalog) != inserted_catalogs.end()) {
				continue;
			}
			inserted_catalogs.insert(catalog);
			catalogs.push_back(catalog);
		}
	} else {
		catalogs.push_back(Catalog::GetCatalog(retriever, catalog_name));
	}
	vector<reference<SchemaCatalogEntry>> result;
	for (auto catalog : catalogs) {
		auto schemas = catalog.get().GetSchemas(retriever.GetContext());
		result.insert(result.end(), schemas.begin(), schemas.end());
	}
	return result;
}

vector<reference<SchemaCatalogEntry>> Catalog::GetSchemas(ClientContext &context, const string &catalog_name) {
	CatalogEntryRetriever retriever(context);
	return GetSchemas(retriever, catalog_name);
}

vector<reference<SchemaCatalogEntry>> Catalog::GetAllSchemas(ClientContext &context) {
	vector<reference<SchemaCatalogEntry>> result;

	auto &db_manager = DatabaseManager::Get(context);
	auto databases = db_manager.GetDatabases(context);
	for (auto &database : databases) {
		if (database->GetVisibility() == AttachVisibility::HIDDEN) {
			continue;
		}
		auto &catalog = database->GetCatalog();
		auto new_schemas = catalog.GetSchemas(context);
		result.insert(result.end(), new_schemas.begin(), new_schemas.end());
	}
	sort(result.begin(), result.end(),
	     [&](reference<SchemaCatalogEntry> left_p, reference<SchemaCatalogEntry> right_p) {
		     auto &left = left_p.get();
		     auto &right = right_p.get();
		     if (left.catalog.GetName() < right.catalog.GetName()) {
			     return true;
		     }
		     if (left.catalog.GetName() == right.catalog.GetName()) {
			     return left.name < right.name;
		     }
		     return false;
	     });

	return result;
}

vector<reference<CatalogEntry>> Catalog::GetAllEntries(ClientContext &context, CatalogType catalog_type) {
	vector<reference<CatalogEntry>> result;
	auto schemas = GetAllSchemas(context);
	for (const auto &schema_ref : schemas) {
		auto &schema = schema_ref.get();
		schema.Scan(context, catalog_type, [&](CatalogEntry &entry) { result.push_back(entry); });
	}
	return result;
}

void Catalog::Alter(CatalogTransaction transaction, AlterInfo &info) {
	if (transaction.HasContext()) {
		CatalogEntryRetriever retriever(transaction.GetContext());
		EntryLookupInfo lookup_info(info.GetCatalogType(), info.name);
		auto lookup = LookupEntry(retriever, info.schema, lookup_info, info.if_not_found);
		if (!lookup.Found()) {
			return;
		}
		return lookup.schema->Alter(transaction, info);
	}
	D_ASSERT(info.if_not_found == OnEntryNotFound::THROW_EXCEPTION);
	auto &schema = GetSchema(transaction, info.schema);
	return schema.Alter(transaction, info);
}

void Catalog::Alter(ClientContext &context, AlterInfo &info) {
	Alter(GetCatalogTransaction(context), info);
}

vector<MetadataBlockInfo> Catalog::GetMetadataInfo(ClientContext &context) {
	return vector<MetadataBlockInfo>();
}

optional_ptr<DependencyManager> Catalog::GetDependencyManager() {
	return nullptr;
}

string Catalog::GetDefaultSchema() const {
	return DEFAULT_SCHEMA;
}

//! Whether this catalog has a default table. Catalogs with a default table can be queries by their catalog name
bool Catalog::HasDefaultTable() const {
	return !default_table.empty();
}

void Catalog::SetDefaultTable(const string &schema, const string &name) {
	default_table = name;
	default_table_schema = schema;
}

string Catalog::GetDefaultTable() const {
	return default_table;
}

string Catalog::GetDefaultTableSchema() const {
	return !default_table_schema.empty() ? default_table_schema : DEFAULT_SCHEMA;
}

void Catalog::Verify() {
}

bool Catalog::IsSystemCatalog() const {
	return db.IsSystem();
}

bool Catalog::IsTemporaryCatalog() const {
	return db.IsTemporary();
}

void Catalog::Initialize(optional_ptr<ClientContext> context, bool load_builtin) {
	Initialize(load_builtin);
}

void Catalog::FinalizeLoad(optional_ptr<ClientContext> context) {
}

void Catalog::OnDetach(ClientContext &context) {
}

bool Catalog::HasConflictingAttachOptions(const string &path, const AttachOptions &options) {
	auto const db_type = options.db_type.empty() ? "duckdb" : options.db_type;
	return GetDBPath() != path || GetCatalogType() != db_type;
}

} // namespace duckdb
