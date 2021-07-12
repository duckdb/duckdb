#include "duckdb/catalog/catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CatalogEntry::CatalogEntry(CatalogType type, Catalog *catalog_p, string name_p)
    : oid(catalog_p->ModifyCatalog()), type(type), catalog(catalog_p), set(nullptr), name(move(name_p)), deleted(false),
      temporary(false), internal(false), parent(nullptr) {
}

CatalogEntry::~CatalogEntry() {
}

unique_ptr<CatalogEntry> CatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) { // LCOV_EXCL_START
	throw CatalogException("Unsupported alter type for catalog entry!");
} // LCOV_EXCL_STOP

unique_ptr<CatalogEntry> CatalogEntry::Copy(ClientContext &context) { // LCOV_EXCL_START
	throw CatalogException("Unsupported copy type for catalog entry!");
} // LCOV_EXCL_STOP

void CatalogEntry::SetAsRoot() {
}

string CatalogEntry::ToSQL() { // LCOV_EXCL_START
	throw CatalogException("Unsupported catalog type for ToSQL()");
} // LCOV_EXCL_STOP

} // namespace duckdb
