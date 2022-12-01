#include "duckdb/catalog/catalog.hpp"

#include "duckdb/catalog/catalog_search_path.hpp"
#include "duckdb/catalog/catalog_entry/list.hpp"
#include "duckdb/catalog/catalog_set.hpp"
#include "duckdb/catalog/default/default_schemas.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parser/expression/function_expression.hpp"
#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/parser/parsed_data/create_aggregate_function_info.hpp"
#include "duckdb/parser/parsed_data/create_collation_info.hpp"
#include "duckdb/parser/parsed_data/create_copy_function_info.hpp"
#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_pragma_function_info.hpp"
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
#include "duckdb/main/extension_functions.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/database_manager.hpp"
#include <algorithm>

namespace duckdb {

string SimilarCatalogEntry::GetQualifiedName() const {
	D_ASSERT(Found());

	return schema->name + "." + name;
}

Catalog::Catalog(AttachedDatabase &db)
    : schemas(make_unique<CatalogSet>(*this, make_unique<DefaultSchemaGenerator>(*this))),
      dependency_manager(make_unique<DependencyManager>(*this)), attached_db(&db), db(nullptr) {
}
Catalog::Catalog(DatabaseInstance &db)
    : schemas(make_unique<CatalogSet>(*this, make_unique<DefaultSchemaGenerator>(*this))),
      dependency_manager(make_unique<DependencyManager>(*this)), attached_db(nullptr), db(&db) {
}
Catalog::~Catalog() {
}

void Catalog::Initialize(bool load_builtin) {
	// first initialize the base system catalogs
	// these are never written to the WAL
	Connection con(GetDatabase());
	con.BeginTransaction();

	// create the default schema
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;
	CreateSchema(*con.context, &info);

	if (load_builtin) {
		// initialize default functions
		BuiltinFunctions builtin(*con.context, *this);
		builtin.Initialize();
	}

	// commit transactions
	con.Commit();
	Verify();
}

DatabaseInstance &Catalog::GetDatabase() {
	if (db) {
		return *db;
	} else {
		D_ASSERT(attached_db);
		return attached_db->GetDatabase();
	}
}

AttachedDatabase &Catalog::GetAttached() {
	if (!attached_db) {
		throw InternalException("Catalog does not have an attached database");
	}
	return *attached_db;
}

Catalog &Catalog::GetSystemCatalog(ClientContext &context) {
	return Catalog::GetSystemCatalog(*context.db);
}

Catalog &Catalog::GetCatalog(ClientContext &context, const string &catalog_name) {
	return Catalog::GetCatalog(*context.db, catalog_name);
}

CatalogEntry *Catalog::CreateTable(ClientContext &context, BoundCreateTableInfo *info) {
	auto schema = GetSchema(context, info->base->schema);
	return CreateTable(context, schema, info);
}

CatalogEntry *Catalog::CreateTable(ClientContext &context, unique_ptr<CreateTableInfo> info) {
	auto binder = Binder::CreateBinder(context);
	auto bound_info = binder->BindCreateTableInfo(move(info));
	return CreateTable(context, bound_info.get());
}

CatalogEntry *Catalog::CreateTable(ClientContext &context, SchemaCatalogEntry *schema, BoundCreateTableInfo *info) {
	return schema->CreateTable(context, info);
}

CatalogEntry *Catalog::CreateView(ClientContext &context, CreateViewInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateView(context, schema, info);
}

CatalogEntry *Catalog::CreateView(ClientContext &context, SchemaCatalogEntry *schema, CreateViewInfo *info) {
	return schema->CreateView(context, info);
}

CatalogEntry *Catalog::CreateSequence(ClientContext &context, CreateSequenceInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateSequence(context, schema, info);
}

CatalogEntry *Catalog::CreateType(ClientContext &context, CreateTypeInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateType(context, schema, info);
}

CatalogEntry *Catalog::CreateSequence(ClientContext &context, SchemaCatalogEntry *schema, CreateSequenceInfo *info) {
	return schema->CreateSequence(context, info);
}

CatalogEntry *Catalog::CreateType(ClientContext &context, SchemaCatalogEntry *schema, CreateTypeInfo *info) {
	return schema->CreateType(context, info);
}

CatalogEntry *Catalog::CreateTableFunction(ClientContext &context, CreateTableFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateTableFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateTableFunction(ClientContext &context, SchemaCatalogEntry *schema,
                                           CreateTableFunctionInfo *info) {
	return schema->CreateTableFunction(context, info);
}

CatalogEntry *Catalog::CreateCopyFunction(ClientContext &context, CreateCopyFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateCopyFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateCopyFunction(ClientContext &context, SchemaCatalogEntry *schema,
                                          CreateCopyFunctionInfo *info) {
	return schema->CreateCopyFunction(context, info);
}

CatalogEntry *Catalog::CreatePragmaFunction(ClientContext &context, CreatePragmaFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreatePragmaFunction(context, schema, info);
}

CatalogEntry *Catalog::CreatePragmaFunction(ClientContext &context, SchemaCatalogEntry *schema,
                                            CreatePragmaFunctionInfo *info) {
	return schema->CreatePragmaFunction(context, info);
}

CatalogEntry *Catalog::CreateFunction(ClientContext &context, CreateFunctionInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateFunction(context, schema, info);
}

CatalogEntry *Catalog::CreateFunction(ClientContext &context, SchemaCatalogEntry *schema, CreateFunctionInfo *info) {
	return schema->CreateFunction(context, info);
}

CatalogEntry *Catalog::CreateCollation(ClientContext &context, CreateCollationInfo *info) {
	auto schema = GetSchema(context, info->schema);
	return CreateCollation(context, schema, info);
}

CatalogEntry *Catalog::CreateCollation(ClientContext &context, SchemaCatalogEntry *schema, CreateCollationInfo *info) {
	return schema->CreateCollation(context, info);
}

CatalogEntry *Catalog::CreateSchema(ClientContext &context, CreateSchemaInfo *info) {
	D_ASSERT(!info->schema.empty());
	if (info->schema == TEMP_SCHEMA) {
		throw CatalogException("Cannot create built-in schema \"%s\"", info->schema);
	}

	unordered_set<CatalogEntry *> dependencies;
	auto entry = make_unique<SchemaCatalogEntry>(this, info->schema, info->internal);
	auto result = entry.get();
	if (!schemas->CreateEntry(context, info->schema, move(entry), dependencies)) {
		if (info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("Schema with name %s already exists!", info->schema);
		} else {
			D_ASSERT(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
		}
		return nullptr;
	}
	return result;
}

void Catalog::DropSchema(ClientContext &context, DropInfo *info) {
	D_ASSERT(!info->name.empty());
	ModifyCatalog();
	if (!schemas->DropEntry(context, info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Schema with name \"%s\" does not exist!", info->name);
		}
	}
}

void Catalog::DropEntry(ClientContext &context, DropInfo *info) {
	ModifyCatalog();
	if (info->type == CatalogType::SCHEMA_ENTRY) {
		// DROP SCHEMA
		DropSchema(context, info);
		return;
	}

	auto lookup = LookupEntry(context, info->type, info->schema, info->name, info->if_exists);
	if (!lookup.Found()) {
		return;
	}

	lookup.schema->DropEntry(context, info);
}

CatalogEntry *Catalog::AddFunction(ClientContext &context, CreateFunctionInfo *info) {
	info->on_conflict = OnCreateConflict::ALTER_ON_CONFLICT;
	return CreateFunction(context, info);
}

SchemaCatalogEntry *Catalog::GetSchema(ClientContext &context, const string &schema_name, bool if_exists,
                                       QueryErrorContext error_context) {
	D_ASSERT(!schema_name.empty());
	if (schema_name == TEMP_SCHEMA) {
		return SchemaCatalogEntry::GetTemporaryObjects(context);
	}
	auto entry = schemas->GetEntry(context, schema_name);
	if (!entry && !if_exists) {
		throw CatalogException(error_context.FormatError("Schema with name %s does not exist!", schema_name));
	}
	return (SchemaCatalogEntry *)entry;
}

void Catalog::ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback) {
	// create all default schemas first
	schemas->Scan(context, [&](CatalogEntry *entry) { callback(entry); });
}

SimilarCatalogEntry Catalog::SimilarEntryInSchemas(ClientContext &context, const string &entry_name, CatalogType type,
                                                   const vector<SchemaCatalogEntry *> &schemas) {

	vector<CatalogSet *> sets;
	std::transform(schemas.begin(), schemas.end(), std::back_inserter(sets),
	               [type](SchemaCatalogEntry *s) -> CatalogSet * { return &s->GetCatalogSet(type); });
	pair<string, idx_t> most_similar {"", (idx_t)-1};
	SchemaCatalogEntry *schema_of_most_similar = nullptr;
	for (auto schema : schemas) {
		auto entry = schema->GetCatalogSet(type).SimilarEntry(context, entry_name);
		if (!entry.first.empty() && (most_similar.first.empty() || most_similar.second > entry.second)) {
			most_similar = entry;
			schema_of_most_similar = schema;
		}
	}

	return {most_similar.first, most_similar.second, schema_of_most_similar};
}

string FindExtension(const string &function_name) {
	auto size = sizeof(EXTENSION_FUNCTIONS) / sizeof(ExtensionFunction);
	auto it = std::lower_bound(
	    EXTENSION_FUNCTIONS, EXTENSION_FUNCTIONS + size, function_name,
	    [](const ExtensionFunction &element, const string &value) { return element.function < value; });
	if (it != EXTENSION_FUNCTIONS + size && it->function == function_name) {
		return it->extension;
	}
	return "";
}
CatalogException Catalog::CreateMissingEntryException(ClientContext &context, const string &entry_name,
                                                      CatalogType type, const vector<SchemaCatalogEntry *> &schemas,
                                                      QueryErrorContext error_context) {
	auto entry = SimilarEntryInSchemas(context, entry_name, type, schemas);

	vector<SchemaCatalogEntry *> unseen_schemas;
	this->schemas->Scan([&schemas, &unseen_schemas](CatalogEntry *entry) {
		auto schema_entry = (SchemaCatalogEntry *)entry;
		if (std::find(schemas.begin(), schemas.end(), schema_entry) == schemas.end()) {
			unseen_schemas.emplace_back(schema_entry);
		}
	});
	auto unseen_entry = SimilarEntryInSchemas(context, entry_name, type, unseen_schemas);
	auto extension_name = FindExtension(entry_name);
	if (!extension_name.empty()) {
		return CatalogException("Function with name %s is not on the catalog, but it exists in the %s extension. To "
		                        "Install and Load the extension, run: INSTALL %s; LOAD %s;",
		                        entry_name, extension_name, extension_name, extension_name);
	}
	string did_you_mean;
	if (unseen_entry.Found() && unseen_entry.distance < entry.distance) {
		did_you_mean = "\nDid you mean \"" + unseen_entry.GetQualifiedName() + "\"?";
	} else if (entry.Found()) {
		did_you_mean = "\nDid you mean \"" + entry.name + "\"?";
	}

	return CatalogException(error_context.FormatError("%s with name %s does not exist!%s", CatalogTypeToString(type),
	                                                  entry_name, did_you_mean));
}

CatalogEntryLookup Catalog::LookupEntry(ClientContext &context, CatalogType type, const string &schema_name,
                                        const string &name, bool if_exists, QueryErrorContext error_context) {
	if (!schema_name.empty()) {
		auto schema = GetSchema(context, schema_name, if_exists, error_context);
		if (!schema) {
			D_ASSERT(if_exists);
			return {nullptr, nullptr};
		}

		auto entry = schema->GetCatalogSet(type).GetEntry(context, name);
		if (!entry && !if_exists) {
			throw CreateMissingEntryException(context, name, type, {schema}, error_context);
		}

		return {schema, entry};
	}

	const auto &paths = ClientData::Get(context).catalog_search_path->Get();
	for (const auto &path : paths) {
		auto lookup = LookupEntry(context, type, path, name, true, error_context);
		if (lookup.Found()) {
			return lookup;
		}
	}

	if (!if_exists) {
		vector<SchemaCatalogEntry *> schemas;
		for (const auto &path : paths) {
			auto schema = GetSchema(context, path, true);
			if (schema) {
				schemas.emplace_back(schema);
			}
		}

		throw CreateMissingEntryException(context, name, type, schemas, error_context);
	}

	return {nullptr, nullptr};
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, const string &schema, const string &name) {
	vector<CatalogType> entry_types {CatalogType::TABLE_ENTRY, CatalogType::SEQUENCE_ENTRY};

	for (auto entry_type : entry_types) {
		CatalogEntry *result = GetEntry(context, entry_type, schema, name, true);
		if (result != nullptr) {
			return result;
		}
	}

	throw CatalogException("CatalogElement \"%s.%s\" does not exist!", schema, name);
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, CatalogType type, const string &schema_name, const string &name,
                                bool if_exists, QueryErrorContext error_context) {
	return LookupEntry(context, type, schema_name, name, if_exists, error_context).entry;
}

vector<Catalog *> GetCatalogs(ClientContext &context, const string &catalog) {
	vector<Catalog *> catalogs;
	catalogs.push_back(&Catalog::GetCatalog(context, catalog));
	if (catalog == INVALID_CATALOG) {
		catalogs.push_back(&Catalog::GetSystemCatalog(context));
	}
	return catalogs;
}

CatalogEntry *Catalog::GetEntry(ClientContext &context, CatalogType type, const string &catalog, const string &schema,
                                const string &name, bool if_exists_p, QueryErrorContext error_context) {
	auto catalogs = GetCatalogs(context, catalog);
	D_ASSERT(!catalogs.empty());
	CatalogEntry *result = nullptr;
	for (idx_t i = 0; i < catalogs.size(); i++) {
		auto if_exists = i + 1 == catalogs.size() ? if_exists_p : true;
		result = catalogs[i]->GetEntry(context, type, schema, name, if_exists, error_context);
		if (result) {
			return result;
		}
	}
	return result;
}

SchemaCatalogEntry *Catalog::GetSchema(ClientContext &context, const string &catalog_name, const string &schema_name,
                                       bool if_exists_p, QueryErrorContext error_context) {
	auto catalogs = GetCatalogs(context, catalog_name);
	D_ASSERT(!catalogs.empty());
	SchemaCatalogEntry *result = nullptr;
	for (idx_t i = 0; i < catalogs.size(); i++) {
		auto if_exists = i + 1 == catalogs.size() ? if_exists_p : true;
		result = catalogs[i]->GetSchema(context, schema_name, if_exists, error_context);
		if (result) {
			return result;
		}
	}
	return result;
}

LogicalType Catalog::GetType(ClientContext &context, const string &catalog_name, const string &schema,
                             const string &name) {
	auto catalogs = GetCatalogs(context, catalog_name);
	D_ASSERT(!catalogs.empty());
	for (idx_t i = 0; i < catalogs.size(); i++) {
		auto if_exists = i + 1 == catalogs.size() ? false : true;
		auto entry = catalogs[i]->GetEntry<TypeCatalogEntry>(context, schema, name, if_exists);
		if (entry) {
			return catalogs[i]->GetType(context, schema, name);
		}
	}
	throw InternalException("Catalog::GetType failed to find type or throw!?");
}

vector<SchemaCatalogEntry *> Catalog::GetSchemas(ClientContext &context, const string &catalog_name) {
	auto catalogs = GetCatalogs(context, catalog_name);
	vector<SchemaCatalogEntry *> result;
	for (auto catalog : catalogs) {
		auto schemas = catalog->schemas->GetEntries<SchemaCatalogEntry>(context);
		result.insert(result.end(), schemas.begin(), schemas.end());
	}
	return result;
}

LogicalType Catalog::GetType(ClientContext &context, const string &schema, const string &name) {
	auto user_type_catalog = GetEntry<TypeCatalogEntry>(context, schema, name);
	auto result_type = user_type_catalog->user_type;
	LogicalType::SetCatalog(result_type, user_type_catalog);
	return result_type;
}

void Catalog::Alter(ClientContext &context, AlterInfo *info) {
	ModifyCatalog();
	auto lookup = LookupEntry(context, info->GetCatalogType(), info->schema, info->name, info->if_exists);
	if (!lookup.Found()) {
		return;
	}
	return lookup.schema->Alter(context, info);
}

void Catalog::Verify() {
#ifdef DEBUG
	schemas->Verify(*this);
#endif
}

idx_t Catalog::GetCatalogVersion() {
	return GetDatabase().GetDatabaseManager().catalog_version;
}

idx_t Catalog::ModifyCatalog() {
	return GetDatabase().GetDatabaseManager().catalog_version++;
}

bool Catalog::IsSystemCatalog() {
	return !attached_db;
}

} // namespace duckdb
