#include "duckdb/common/type_visitor.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry.hpp"

namespace duckdb {
namespace {

struct CCatalogWrapper {
	Catalog &catalog;
	string catalog_type;
	CCatalogWrapper(Catalog &catalog, const string &catalog_type) : catalog(catalog), catalog_type(catalog_type) {
	}
};

static CatalogType CatalogTypeFromC(duckdb_catalog_entry_type type) {
	switch (type) {
	case DUCKDB_CATALOG_ENTRY_TYPE_TABLE:
		return CatalogType::TABLE_ENTRY;
	case DUCKDB_CATALOG_ENTRY_TYPE_SCHEMA:
		return CatalogType::SCHEMA_ENTRY;
	case DUCKDB_CATALOG_ENTRY_TYPE_VIEW:
		return CatalogType::VIEW_ENTRY;
	case DUCKDB_CATALOG_ENTRY_TYPE_INDEX:
		return CatalogType::INDEX_ENTRY;
	case DUCKDB_CATALOG_ENTRY_TYPE_PREPARED_STATEMENT:
		return CatalogType::PREPARED_STATEMENT;
	case DUCKDB_CATALOG_ENTRY_TYPE_SEQUENCE:
		return CatalogType::SEQUENCE_ENTRY;
	case DUCKDB_CATALOG_ENTRY_TYPE_COLLATION:
		return CatalogType::COLLATION_ENTRY;
	case DUCKDB_CATALOG_ENTRY_TYPE_TYPE:
		return CatalogType::TYPE_ENTRY;
	case DUCKDB_CATALOG_ENTRY_TYPE_DATABASE:
		return CatalogType::DATABASE_ENTRY;
	default:
		return CatalogType::INVALID;
	}
}

static duckdb_catalog_entry_type CatalogTypeToC(CatalogType type) {
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return DUCKDB_CATALOG_ENTRY_TYPE_TABLE;
	case CatalogType::SCHEMA_ENTRY:
		return DUCKDB_CATALOG_ENTRY_TYPE_SCHEMA;
	case CatalogType::VIEW_ENTRY:
		return DUCKDB_CATALOG_ENTRY_TYPE_VIEW;
	case CatalogType::INDEX_ENTRY:
		return DUCKDB_CATALOG_ENTRY_TYPE_INDEX;
	case CatalogType::PREPARED_STATEMENT:
		return DUCKDB_CATALOG_ENTRY_TYPE_PREPARED_STATEMENT;
	case CatalogType::SEQUENCE_ENTRY:
		return DUCKDB_CATALOG_ENTRY_TYPE_SEQUENCE;
	case CatalogType::COLLATION_ENTRY:
		return DUCKDB_CATALOG_ENTRY_TYPE_COLLATION;
	case CatalogType::TYPE_ENTRY:
		return DUCKDB_CATALOG_ENTRY_TYPE_TYPE;
	case CatalogType::DATABASE_ENTRY:
		return DUCKDB_CATALOG_ENTRY_TYPE_DATABASE;
	default:
		return DUCKDB_CATALOG_ENTRY_TYPE_INVALID;
	}
}

} // namespace
} // namespace duckdb

//----------------------------------------------------------------------------------------------------------------------
// Catalog
//----------------------------------------------------------------------------------------------------------------------
duckdb_catalog duckdb_client_context_get_catalog(duckdb_client_context context, const char *name) {
	if (!context || !name || strlen(name) == 0) {
		return nullptr;
	}

	auto &context_ref = *reinterpret_cast<duckdb::CClientContextWrapper *>(context);
	auto &catalog = duckdb::Catalog::GetCatalog(context_ref.context, name);

	auto catalog_ptr = new duckdb::CCatalogWrapper(catalog, catalog.GetCatalogType());
	return reinterpret_cast<duckdb_catalog>(catalog_ptr);
}

void duckdb_destroy_catalog(duckdb_catalog *catalog) {
	if (!catalog || !*catalog) {
		return;
	}
	auto catalog_ptr = reinterpret_cast<duckdb::CCatalogWrapper *>(*catalog);
	delete catalog_ptr;
	*catalog = nullptr;
}

const char *duckdb_catalog_get_type(duckdb_catalog catalog) {
	if (!catalog) {
		return nullptr;
	}
	auto &catalog_ref = *reinterpret_cast<duckdb::CCatalogWrapper *>(catalog);
	return catalog_ref.catalog_type.c_str();
}

duckdb_catalog_entry duckdb_catalog_get_entry(duckdb_catalog catalog, duckdb_client_context context,
                                              duckdb_catalog_entry_type entry_type, const char *schema_name,
                                              const char *entry_name) {
	if (!catalog) {
		return nullptr;
	}
	auto &catalog_ref = *reinterpret_cast<duckdb::CCatalogWrapper *>(catalog);
	auto &context_ref = *reinterpret_cast<duckdb::CClientContextWrapper *>(context);

	auto entry = catalog_ref.catalog.GetEntry(context_ref.context, duckdb::CatalogTypeFromC(entry_type), schema_name,
	                                          entry_name, duckdb::OnEntryNotFound::RETURN_NULL);

	if (!entry) {
		return nullptr;
	}

	return reinterpret_cast<duckdb_catalog_entry>(entry.get());
}

//----------------------------------------------------------------------------------------------------------------------
// Catalog Entry
//----------------------------------------------------------------------------------------------------------------------

duckdb_catalog_entry_type duckdb_catalog_entry_get_type(duckdb_catalog_entry entry) {
	if (!entry) {
		return DUCKDB_CATALOG_ENTRY_TYPE_INVALID;
	}

	// TODO!
	auto &entry_ref = *reinterpret_cast<duckdb::CatalogEntry *>(entry);
	return duckdb::CatalogTypeToC(entry_ref.type);
}

const char *duckdb_catalog_entry_get_name(duckdb_catalog_entry entry) {
	if (!entry) {
		return nullptr;
	}
	auto &entry_ref = *reinterpret_cast<duckdb::CatalogEntry *>(entry);
	return entry_ref.name.c_str();
}

void duckdb_destroy_catalog_entry(duckdb_catalog_entry *entry) {
	if (!entry || !*entry) {
		return;
	}
	// Catalog entries are not owned, so we do not delete them here.
	*entry = nullptr;
}
