#include "duckdb/catalog/dcatalog.hpp"
#include "duckdb/catalog/dependency_manager.hpp"
#include "duckdb/catalog/catalog_entry/dschema_catalog_entry.hpp"

namespace duckdb {

DCatalog::DCatalog(AttachedDatabase &db)
    : Catalog(db), dependency_manager(make_unique<DependencyManager>(*this)),
      schemas(make_unique<CatalogSet>(*this, make_unique<DefaultSchemaGenerator>(*this))) {
}

DCatalog::~DCatalog() {
}

void DCatalog::Initialize(bool load_builtin) {
	// first initialize the base system catalogs
	// these are never written to the WAL
	// we start these at 1 because deleted entries default to 0
	CatalogTransaction data(GetDatabase(), 1, 1);

	// create the default schema
	CreateSchemaInfo info;
	info.schema = DEFAULT_SCHEMA;
	info.internal = true;
	CreateSchema(data, &info);

	if (load_builtin) {
		// initialize default functions
		BuiltinFunctions builtin(data, *this);
		builtin.Initialize();
	}

	Verify();
}

bool DCatalog::IsDCatalog() {
	return true;
}

//===--------------------------------------------------------------------===//
// Schema
//===--------------------------------------------------------------------===//
CatalogEntry *DCatalog::CreateSchema(CatalogTransaction transaction, CreateSchemaInfo *info) {
	D_ASSERT(!info->schema.empty());
	DependencyList dependencies;
	auto entry = make_unique<DSchemaCatalogEntry>(this, info->schema, info->internal);
	auto result = entry.get();
	if (!schemas->CreateEntry(transaction, info->schema, std::move(entry), dependencies)) {
		if (info->on_conflict == OnCreateConflict::ERROR_ON_CONFLICT) {
			throw CatalogException("Schema with name %s already exists!", info->schema);
		} else {
			D_ASSERT(info->on_conflict == OnCreateConflict::IGNORE_ON_CONFLICT);
		}
		return nullptr;
	}
	return result;
}

void DCatalog::DropSchema(ClientContext &context, DropInfo *info) {
	D_ASSERT(!info->name.empty());
	ModifyCatalog();
	if (!schemas->DropEntry(GetCatalogTransaction(context), info->name, info->cascade)) {
		if (!info->if_exists) {
			throw CatalogException("Schema with name \"%s\" does not exist!", info->name);
		}
	}
}

void DCatalog::ScanSchemas(ClientContext &context, std::function<void(CatalogEntry *)> callback) {
	schemas->Scan(GetCatalogTransaction(context), [&](CatalogEntry *entry) { callback(entry); });
}

void DCatalog::ScanSchemas(std::function<void(CatalogEntry *)> callback) {
	schemas->Scan([&](CatalogEntry *entry) { callback(entry); });
}

SchemaCatalogEntry *DCatalog::GetSchema(CatalogTransaction transaction, const string &schema_name, bool if_exists,
                                        QueryErrorContext error_context) {
	D_ASSERT(!schema_name.empty());
	auto entry = schemas->GetEntry(transaction, schema_name);
	if (!entry && !if_exists) {
		throw CatalogException(error_context.FormatError("Schema with name %s does not exist!", schema_name));
	}
	return (SchemaCatalogEntry *)entry;
}

void DCatalog::Verify() {
#ifdef DEBUG
	schemas->Verify(*this);
#endif
}

} // namespace duckdb
