#include "duckdb/catalog/catalog_entry.hpp"

#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CatalogEntry::CatalogEntry(CatalogType type, Catalog *catalog_p, string name_p)
    : oid(catalog_p->ModifyCatalog()), type(type), catalog(catalog_p), set(nullptr), name(move(name_p)), deleted(false),
      temporary(false), internal(false), parent(nullptr) {
}

CatalogEntry::~CatalogEntry() {
}

} // namespace duckdb
