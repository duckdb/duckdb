#include "duckdb/catalog/catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CatalogEntry::CatalogEntry(CatalogType type, Catalog *catalog_p, string name_p)
    : oid(catalog_p ? catalog_p->ModifyCatalog() : 0), type(type), catalog(catalog_p), set(nullptr),
      name(std::move(name_p)), deleted(false), temporary(false), internal(false), parent(nullptr) {
}

CatalogEntry::~CatalogEntry() {
}

void CatalogEntry::SetAsRoot() {
}

// LCOV_EXCL_START
unique_ptr<CatalogEntry> CatalogEntry::AlterEntry(ClientContext &context, AlterInfo *info) {
	throw InternalException("Unsupported alter type for catalog entry!");
}

void CatalogEntry::UndoAlter(ClientContext &context, AlterInfo *info) {
}

unique_ptr<CatalogEntry> CatalogEntry::Copy(ClientContext &context) {
	throw InternalException("Unsupported copy type for catalog entry!");
}

string CatalogEntry::ToSQL() {
	throw InternalException("Unsupported catalog type for ToSQL()");
}
// LCOV_EXCL_STOP

void CatalogEntry::Verify(Catalog &catalog_p) {
	D_ASSERT(&catalog_p == catalog);
}

} // namespace duckdb
