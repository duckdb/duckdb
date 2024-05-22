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
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/catalog/default/default_types.hpp"
#include "duckdb/main/extension_entries.hpp"
#include "duckdb/main/extension/generated_extension_loader.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/function/built_in_functions.hpp"
#include "duckdb/catalog/similar_catalog_entry.hpp"
#include "duckdb/storage/database_size.hpp"
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

optional_ptr<Catalog> Catalog::GetCatalogEntry(ClientContext &context, const string &catalog_name) {
	auto &db_manager = DatabaseManager::Get(context);
	if (catalog_name == TEMP_CATALOG) {
		return &ClientData::Get(context).temporary_objects->GetCatalog();
	}
	if (catalog_name == SYSTEM_CATALOG) {
		return &GetSystemCatalog(context);
	}
	auto entry = db_manager.GetDatabase(
	    context, IsInvalidCatalog(catalog_name) ? DatabaseManager::GetDefaultDatabase(context) : catalog_name);
	if (!entry) {
		return nullptr;
	}
	return &entry->GetCatalog();
}

Catalog &Catalog::GetCatalog(ClientContext &context, const string &catalog_name) {
	auto catalog = Catalog::GetCatalogEntry(context, catalog_name);
	if (!catalog) {
		throw BinderException("Catalog \"%s\" does not exist!", catalog_name);
	}
	return *catalog;
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

//===--------------------------------------------------------------------===//
// Lookup Structures
//===--------------------------------------------------------------------===//
struct CatalogLookup {
	CatalogLookup(Catalog &catalog, string schema_p) : catalog(catalog), schema(std::move(schema_p)) {
	}

	Catalog &catalog;
	string schema;
};

//! Return value of Catalog::LookupEntry
struct CatalogEntryLookup {
	optional_ptr<SchemaCatalogEntry> schema;
	optional_ptr<CatalogEntry> entry;
	ErrorData error;

	DUCKDB_API bool Found() const {
		return entry;
	}
};

//===--------------------------------------------------------------------===//
// Generic
//===--------------------------------------------------------------------===//
void Catalog::DropEntry(ClientContext &context, DropInfo &info) {
	ModifyCatalog();
	if (info.type == CatalogType::SCHEMA_ENTRY) {
		// DROP SCHEMA
		DropSchema(context, info);
		return;
	}

	auto lookup = LookupEntry(context, info.type, info.schema, info.name, info.if_not_found);

	if (!lookup.Found()) {
		return;
	}

	lookup.schema->DropEntry(context, info);
}

SchemaCatalogEntry &Catalog::GetSchema(ClientContext &context, const string &name, QueryErrorContext error_context) {
	return *Catalog::GetSchema(context, name, OnEntryNotFound::THROW_EXCEPTION, error_context);
}

optional_ptr<SchemaCatalogEntry> Catalog::GetSchema(ClientContext &context, const string &schema_name,
                                                    OnEntryNotFound if_not_found, QueryErrorContext error_context) {
	return GetSchema(GetCatalogTransaction(context), schema_name, if_not_found, error_context);
}

SchemaCatalogEntry &Catalog::GetSchema(ClientContext &context, const string &catalog_name, const string &schema_name,
                                       QueryErrorContext error_context) {
	return *Catalog::GetSchema(context, catalog_name, schema_name, OnEntryNotFound::THROW_EXCEPTION, error_context);
}

SchemaCatalogEntry &Catalog::GetSchema(CatalogTransaction transaction, const string &name,
                                       QueryErrorContext error_context) {
	return *GetSchema(transaction, name, OnEntryNotFound::THROW_EXCEPTION, error_context);
}

//===--------------------------------------------------------------------===//
// Lookup
//===--------------------------------------------------------------------===//
SimilarCatalogEntry Catalog::SimilarEntryInSchemas(ClientContext &context, const string &entry_name, CatalogType type,
                                                   const reference_set_t<SchemaCatalogEntry> &schemas) {
	SimilarCatalogEntry result;
	for (auto schema_ref : schemas) {
		auto &schema = schema_ref.get();
		auto transaction = schema.catalog.GetCatalogTransaction(context);
		auto entry = schema.GetSimilarEntry(transaction, type, entry_name);
		if (!entry.Found()) {
			// no similar entry found
			continue;
		}
		if (!result.Found() || result.distance > entry.distance) {
			result = entry;
			result.schema = &schema;
		}
	}
	return result;
}

vector<CatalogSearchEntry> GetCatalogEntries(ClientContext &context, const string &catalog, const string &schema) {
	vector<CatalogSearchEntry> entries;
	auto &search_path = *context.client_data->catalog_search_path;
	if (IsInvalidCatalog(catalog) && IsInvalidSchema(schema)) {
		// no catalog or schema provided - scan the entire search path
		entries = search_path.Get();
	} else if (IsInvalidCatalog(catalog)) {
		auto catalogs = search_path.GetCatalogsForSchema(schema);
		for (auto &catalog_name : catalogs) {
			entries.emplace_back(catalog_name, schema);
		}
		if (entries.empty()) {
			entries.emplace_back(DatabaseManager::GetDefaultDatabase(context), schema);
		}
	} else if (IsInvalidSchema(schema)) {
		auto schemas = search_path.GetSchemasForCatalog(catalog);
		for (auto &schema_name : schemas) {
			entries.emplace_back(catalog, schema_name);
		}
		if (entries.empty()) {
			entries.emplace_back(catalog, DEFAULT_SCHEMA);
		}
	} else {
		// specific catalog and schema provided
		entries.emplace_back(catalog, schema);
	}
	return entries;
}

void FindMinimalQualification(ClientContext &context, const string &catalog_name, const string &schema_name,
                              bool &qualify_database, bool &qualify_schema) {
	// check if we can we qualify ONLY the schema
	bool found = false;
	auto entries = GetCatalogEntries(context, INVALID_CATALOG, schema_name);
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
	entries = GetCatalogEntries(context, catalog_name, INVALID_SCHEMA);
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

void Catalog::AutoloadExtensionByConfigName(ClientContext &context, const string &configuration_name) {
#ifndef DUCKDB_DISABLE_EXTENSION_LOAD
	auto &dbconfig = DBConfig::GetConfig(context);
	if (dbconfig.options.autoload_known_extensions) {
		auto extension_name = ExtensionHelper::FindExtensionInEntries(configuration_name, EXTENSION_SETTINGS);
		if (ExtensionHelper::CanAutoloadExtension(extension_name)) {
			ExtensionHelper::AutoLoadExtension(context, extension_name);
			return;
		}
	}
#endif

	throw Catalog::UnrecognizedConfigurationError(context, configuration_name);
}

static bool IsAutoloadableFunction(CatalogType type) {
	return (type == CatalogType::TABLE_FUNCTION_ENTRY || type == CatalogType::SCALAR_FUNCTION_ENTRY ||
	        type == CatalogType::AGGREGATE_FUNCTION_ENTRY || type == CatalogType::PRAGMA_FUNCTION_ENTRY);
}

static bool CompareCatalogTypes(CatalogType type_a, CatalogType type_b) {
	if (type_a == type_b) {
		// Types are same
		return true;
	}
	if (!IsAutoloadableFunction(type_a)) {
		D_ASSERT(IsAutoloadableFunction(type_b));
		// Make sure that `type_a` is an autoloadable function
		return CompareCatalogTypes(type_b, type_a);
	}
	if (type_a == CatalogType::TABLE_FUNCTION_ENTRY) {
		// These are all table functions
		return type_b == CatalogType::TABLE_MACRO_ENTRY || type_b == CatalogType::PRAGMA_FUNCTION_ENTRY;
	} else if (type_a == CatalogType::SCALAR_FUNCTION_ENTRY) {
		// These are all scalar functions
		return type_b == CatalogType::MACRO_ENTRY;
	} else if (type_a == CatalogType::PRAGMA_FUNCTION_ENTRY) {
		// These are all table functions
		return type_b == CatalogType::TABLE_MACRO_ENTRY || type_b == CatalogType::TABLE_FUNCTION_ENTRY;
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

CatalogException Catalog::CreateMissingEntryException(ClientContext &context, const string &entry_name,
                                                      CatalogType type,
                                                      const reference_set_t<SchemaCatalogEntry> &schemas,
                                                      QueryErrorContext error_context) {
	auto entry = SimilarEntryInSchemas(context, entry_name, type, schemas);

	reference_set_t<SchemaCatalogEntry> unseen_schemas;
	auto &db_manager = DatabaseManager::Get(context);
	auto databases = db_manager.GetDatabases(context);
	for (auto database : databases) {
		auto &catalog = database.get().GetCatalog();
		auto current_schemas = catalog.GetAllSchemas(context);
		for (auto &current_schema : current_schemas) {
			unseen_schemas.insert(current_schema.get());
		}
	}
	// check if the entry exists in any extension
	string extension_name;
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

	auto unseen_entry = SimilarEntryInSchemas(context, entry_name, type, unseen_schemas);
	string did_you_mean;
	if (unseen_entry.Found() && unseen_entry.distance < entry.distance) {
		// the closest matching entry requires qualification as it is not in the default search path
		// check how to minimally qualify this entry
		auto catalog_name = unseen_entry.schema->catalog.GetName();
		auto schema_name = unseen_entry.schema->name;
		bool qualify_database;
		bool qualify_schema;
		FindMinimalQualification(context, catalog_name, schema_name, qualify_database, qualify_schema);
		did_you_mean = unseen_entry.GetQualifiedName(qualify_database, qualify_schema);
	} else if (entry.Found()) {
		did_you_mean = entry.name;
	}
	return CatalogException::MissingEntry(type, entry_name, did_you_mean, error_context);
}

CatalogEntryLookup Catalog::TryLookupEntryInternal(CatalogTransaction transaction, CatalogType type,
                                                   const string &schema, const string &name) {
	auto schema_entry = GetSchema(transaction, schema, OnEntryNotFound::RETURN_NULL);
	if (!schema_entry) {
		return {nullptr, nullptr, ErrorData()};
	}
	auto entry = schema_entry->GetEntry(transaction, type, name);
	if (!entry) {
		return {schema_entry, nullptr, ErrorData()};
	}
	return {schema_entry, entry, ErrorData()};
}

CatalogEntryLookup Catalog::TryLookupEntry(ClientContext &context, CatalogType type, const string &schema,
                                           const string &name, OnEntryNotFound if_not_found,
                                           QueryErrorContext error_context) {
	reference_set_t<SchemaCatalogEntry> schemas;
	if (IsInvalidSchema(schema)) {
		// try all schemas for this catalog
		auto entries = GetCatalogEntries(context, GetName(), INVALID_SCHEMA);
		for (auto &entry : entries) {
			auto &candidate_schema = entry.schema;
			auto transaction = GetCatalogTransaction(context);
			auto result = TryLookupEntryInternal(transaction, type, candidate_schema, name);
			if (result.Found()) {
				return result;
			}
			if (result.schema) {
				schemas.insert(*result.schema);
			}
		}
	} else {
		auto transaction = GetCatalogTransaction(context);
		auto result = TryLookupEntryInternal(transaction, type, schema, name);
		if (result.Found()) {
			return result;
		}
		if (result.schema) {
			schemas.insert(*result.schema);
		}
	}

	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return {nullptr, nullptr, ErrorData()};
	} else {
		auto except = CreateMissingEntryException(context, name, type, schemas, error_context);
		return {nullptr, nullptr, ErrorData(except)};
	}
}

CatalogEntryLookup Catalog::LookupEntry(ClientContext &context, CatalogType type, const string &schema,
                                        const string &name, OnEntryNotFound if_not_found,
                                        QueryErrorContext error_context) {
	auto res = TryLookupEntry(context, type, schema, name, if_not_found, error_context);

	if (res.error.HasError()) {
		res.error.Throw();
	}

	return res;
}

CatalogEntryLookup Catalog::TryLookupEntry(ClientContext &context, vector<CatalogLookup> &lookups, CatalogType type,
                                           const string &name, OnEntryNotFound if_not_found,
                                           QueryErrorContext error_context) {
	reference_set_t<SchemaCatalogEntry> schemas;
	for (auto &lookup : lookups) {
		auto transaction = lookup.catalog.GetCatalogTransaction(context);
		auto result = lookup.catalog.TryLookupEntryInternal(transaction, type, lookup.schema, name);
		if (result.Found()) {
			return result;
		}
		if (result.schema) {
			schemas.insert(*result.schema);
		}
	}

	if (if_not_found == OnEntryNotFound::RETURN_NULL) {
		return {nullptr, nullptr, ErrorData()};
	} else {
		auto except = CreateMissingEntryException(context, name, type, schemas, error_context);
		return {nullptr, nullptr, ErrorData(except)};
	}
}

CatalogEntryLookup Catalog::TryLookupEntry(ClientContext &context, CatalogType type, const string &catalog,
                                           const string &schema, const string &name, OnEntryNotFound if_not_found,
                                           QueryErrorContext error_context) {
	auto entries = GetCatalogEntries(context, catalog, schema);
	vector<CatalogLookup> lookups;
	vector<CatalogLookup> final_lookups;
	lookups.reserve(entries.size());
	for (auto &entry : entries) {
		optional_ptr<Catalog> catalog_entry;
		if (if_not_found == OnEntryNotFound::RETURN_NULL) {
			catalog_entry = Catalog::GetCatalogEntry(context, entry.catalog);
		} else {
			catalog_entry = &Catalog::GetCatalog(context, entry.catalog);
		}
		if (!catalog_entry) {
			return {nullptr, nullptr, ErrorData()};
		}
		D_ASSERT(catalog_entry);
		auto lookup_behavior = catalog_entry->CatalogTypeLookupRule(type);
		if (lookup_behavior == CatalogLookupBehavior::STANDARD) {
			lookups.emplace_back(*catalog_entry, entry.schema);
		} else if (lookup_behavior == CatalogLookupBehavior::LOWER_PRIORITY) {
			final_lookups.emplace_back(*catalog_entry, entry.schema);
		}
	}
	for (auto &lookup : final_lookups) {
		lookups.emplace_back(std::move(lookup));
	}
	return Catalog::TryLookupEntry(context, lookups, type, name, if_not_found, error_context);
}

optional_ptr<CatalogEntry> Catalog::GetEntry(ClientContext &context, CatalogType type, const string &schema_name,
                                             const string &name, OnEntryNotFound if_not_found,
                                             QueryErrorContext error_context) {
	auto lookup_entry = TryLookupEntry(context, type, schema_name, name, if_not_found, error_context);

	// Try autoloading extension to resolve lookup
	if (!lookup_entry.Found()) {
		if (AutoLoadExtensionByCatalogEntry(*context.db, type, name)) {
			lookup_entry = TryLookupEntry(context, type, schema_name, name, if_not_found, error_context);
		}
	}

	if (lookup_entry.error.HasError()) {
		lookup_entry.error.Throw();
	}

	return lookup_entry.entry.get();
}

CatalogEntry &Catalog::GetEntry(ClientContext &context, CatalogType type, const string &schema, const string &name,
                                QueryErrorContext error_context) {
	return *Catalog::GetEntry(context, type, schema, name, OnEntryNotFound::THROW_EXCEPTION, error_context);
}

optional_ptr<CatalogEntry> Catalog::GetEntry(ClientContext &context, CatalogType type, const string &catalog,
                                             const string &schema, const string &name, OnEntryNotFound if_not_found,
                                             QueryErrorContext error_context) {
	auto result = TryLookupEntry(context, type, catalog, schema, name, if_not_found, error_context);

	// Try autoloading extension to resolve lookup
	if (!result.Found()) {
		if (AutoLoadExtensionByCatalogEntry(*context.db, type, name)) {
			result = TryLookupEntry(context, type, catalog, schema, name, if_not_found, error_context);
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

CatalogEntry &Catalog::GetEntry(ClientContext &context, CatalogType type, const string &catalog, const string &schema,
                                const string &name, QueryErrorContext error_context) {
	return *Catalog::GetEntry(context, type, catalog, schema, name, OnEntryNotFound::THROW_EXCEPTION, error_context);
}

optional_ptr<SchemaCatalogEntry> Catalog::GetSchema(ClientContext &context, const string &catalog_name,
                                                    const string &schema_name, OnEntryNotFound if_not_found,
                                                    QueryErrorContext error_context) {
	auto entries = GetCatalogEntries(context, catalog_name, schema_name);
	for (idx_t i = 0; i < entries.size(); i++) {
		auto on_not_found = i + 1 == entries.size() ? if_not_found : OnEntryNotFound::RETURN_NULL;
		auto &catalog = Catalog::GetCatalog(context, entries[i].catalog);
		auto result = catalog.GetSchema(context, schema_name, on_not_found, error_context);
		if (result) {
			return result;
		}
	}
	return nullptr;
}

vector<reference<SchemaCatalogEntry>> Catalog::GetSchemas(ClientContext &context) {
	vector<reference<SchemaCatalogEntry>> schemas;
	ScanSchemas(context, [&](SchemaCatalogEntry &entry) { schemas.push_back(entry); });
	return schemas;
}

vector<reference<SchemaCatalogEntry>> Catalog::GetSchemas(ClientContext &context, const string &catalog_name) {
	vector<reference<Catalog>> catalogs;
	if (IsInvalidCatalog(catalog_name)) {
		reference_set_t<Catalog> inserted_catalogs;

		auto &search_path = *context.client_data->catalog_search_path;
		for (auto &entry : search_path.Get()) {
			auto &catalog = Catalog::GetCatalog(context, entry.catalog);
			if (inserted_catalogs.find(catalog) != inserted_catalogs.end()) {
				continue;
			}
			inserted_catalogs.insert(catalog);
			catalogs.push_back(catalog);
		}
	} else {
		catalogs.push_back(Catalog::GetCatalog(context, catalog_name));
	}
	vector<reference<SchemaCatalogEntry>> result;
	for (auto catalog : catalogs) {
		auto schemas = catalog.get().GetSchemas(context);
		result.insert(result.end(), schemas.begin(), schemas.end());
	}
	return result;
}

vector<reference<SchemaCatalogEntry>> Catalog::GetAllSchemas(ClientContext &context) {
	vector<reference<SchemaCatalogEntry>> result;

	auto &db_manager = DatabaseManager::Get(context);
	auto databases = db_manager.GetDatabases(context);
	for (auto database : databases) {
		auto &catalog = database.get().GetCatalog();
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

void Catalog::Alter(CatalogTransaction transaction, AlterInfo &info) {
	ModifyCatalog();

	if (transaction.HasContext()) {
		auto lookup =
		    LookupEntry(transaction.GetContext(), info.GetCatalogType(), info.schema, info.name, info.if_not_found);
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

void Catalog::Verify() {
}

//===--------------------------------------------------------------------===//
// Catalog Version
//===--------------------------------------------------------------------===//
idx_t Catalog::GetCatalogVersion() {
	return GetDatabase().GetDatabaseManager().catalog_version;
}

idx_t Catalog::ModifyCatalog() {
	return GetDatabase().GetDatabaseManager().ModifyCatalog();
}

bool Catalog::IsSystemCatalog() const {
	return db.IsSystem();
}

bool Catalog::IsTemporaryCatalog() const {
	return db.IsTemporary();
}

} // namespace duckdb
