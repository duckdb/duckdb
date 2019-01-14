#include "catalog/catalog_entry/view_catalog_entry.hpp"

#include "catalog/catalog.hpp"
#include "common/exception.hpp"
#include "common/serializer.hpp"
#include "parser/constraints/list.hpp"
#include "storage/storage_manager.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

void ViewCatalogEntry::Initialize(CreateViewInformation *info) {
	query = move(info->query);
}

ViewCatalogEntry::ViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateViewInformation *info)
    : CatalogEntry(CatalogType::VIEW, catalog, info->table), schema(schema) {
	Initialize(info);
}

void ViewCatalogEntry::Serialize(Serializer &serializer) {
	throw NotImplementedException("");
}

unique_ptr<CreateViewInformation> ViewCatalogEntry::Deserialize(Deserializer &source) {
	throw NotImplementedException("");
}
