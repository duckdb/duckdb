#include "duckdb/catalog/catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CatalogEntry::CatalogEntry(CatalogType type, string name_p, idx_t oid)
    : oid(oid), type(type), set(nullptr), name(std::move(name_p)), deleted(false), temporary(false), internal(false),
      parent(nullptr) {
}

CatalogEntry::CatalogEntry(CatalogType type, Catalog &catalog, string name_p)
    : CatalogEntry(type, std::move(name_p), catalog.ModifyCatalog()) {
}

CatalogEntry::~CatalogEntry() {
}

void CatalogEntry::SetAsRoot() {
}

// LCOV_EXCL_START
unique_ptr<CatalogEntry> CatalogEntry::AlterEntry(ClientContext &context, AlterInfo &info) {
	throw InternalException("Unsupported alter type for catalog entry!");
}

void CatalogEntry::UndoAlter(ClientContext &context, AlterInfo &info) {
}

unique_ptr<CatalogEntry> CatalogEntry::Copy(ClientContext &context) const {
	throw InternalException("Unsupported copy type for catalog entry!");
}

string CatalogEntry::ToSQL() const {
	throw InternalException("Unsupported catalog type for ToSQL()");
}
// LCOV_EXCL_STOP

void CatalogEntry::Verify(Catalog &catalog_p) {
}

Catalog &CatalogEntry::ParentCatalog() {
	throw InternalException("CatalogEntry::ParentCatalog called on catalog entry without catalog");
}

SchemaCatalogEntry &CatalogEntry::ParentSchema() {
	throw InternalException("CatalogEntry::ParentSchema called on catalog entry without schema");
}

InCatalogEntry::InCatalogEntry(CatalogType type, Catalog &catalog, string name)
    : CatalogEntry(type, catalog, std::move(name)), catalog(catalog) {
}

InCatalogEntry::~InCatalogEntry() {
}

void InCatalogEntry::Verify(Catalog &catalog_p) {
	D_ASSERT(&catalog_p == &catalog);
}
} // namespace duckdb
