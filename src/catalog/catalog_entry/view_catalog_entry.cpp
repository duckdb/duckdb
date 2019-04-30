#include "catalog/catalog_entry/view_catalog_entry.hpp"

#include "catalog/catalog_entry/schema_catalog_entry.hpp"
#include "common/exception.hpp"
#include "common/serializer.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

void ViewCatalogEntry::Initialize(CreateViewInformation *info) {
	query = move(info->query);
	aliases = info->aliases;
}

ViewCatalogEntry::ViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateViewInformation *info)
    : CatalogEntry(CatalogType::VIEW, catalog, info->view_name), schema(schema) {
	Initialize(info);
}

void ViewCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	query->Serialize(serializer);
	serializer.Write<uint32_t>(aliases.size());
	for (auto s : aliases) {
		serializer.WriteString(s);
	}
}

unique_ptr<CreateViewInformation> ViewCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateViewInformation>();
	info->schema = source.Read<string>();
	info->view_name = source.Read<string>();
	info->query = QueryNode::Deserialize(source);
	auto alias_count = source.Read<uint32_t>();
	for (size_t i = 0; i < alias_count; i++) {
		info->aliases.push_back(source.Read<string>());
	}
	return info;
}
