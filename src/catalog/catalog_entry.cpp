#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/common/serializer/binary_serializer.hpp"
#include "duckdb/common/serializer/binary_deserializer.hpp"

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

unique_ptr<CreateInfo> CatalogEntry::GetInfo() const {
	throw InternalException("Unsupported type for CatalogEntry::GetInfo!");
}

string CatalogEntry::ToSQL() const {
	throw InternalException("Unsupported catalog type for ToSQL()");
}

Catalog &CatalogEntry::ParentCatalog() {
	throw InternalException("CatalogEntry::ParentCatalog called on catalog entry without catalog");
}

SchemaCatalogEntry &CatalogEntry::ParentSchema() {
	throw InternalException("CatalogEntry::ParentSchema called on catalog entry without schema");
}
// LCOV_EXCL_STOP

void CatalogEntry::Serialize(Serializer &serializer) const {
	const auto info = GetInfo();
	info->Serialize(serializer);
}

unique_ptr<CreateInfo> CatalogEntry::Deserialize(Deserializer &deserializer) {
	return CreateInfo::Deserialize(deserializer);
}

void CatalogEntry::Verify(Catalog &catalog_p) {
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
