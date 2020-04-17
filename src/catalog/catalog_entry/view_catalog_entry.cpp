#include "duckdb/catalog/catalog_entry/view_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"

#include <algorithm>

using namespace duckdb;
using namespace std;

void ViewCatalogEntry::Initialize(CreateViewInfo *info) {
	query = move(info->query);
	this->aliases = info->aliases;
	this->types = info->types;
	this->temporary = info->temporary;
}

ViewCatalogEntry::ViewCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateViewInfo *info)
    : StandardEntry(CatalogType::VIEW, schema, catalog, info->view_name) {
	Initialize(info);
}

void ViewCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	query->Serialize(serializer);
	assert(aliases.size() <= numeric_limits<uint32_t>::max());
	serializer.Write<uint32_t>((uint32_t)aliases.size());
	for (auto &alias : aliases) {
		serializer.WriteString(alias);
	}
	serializer.Write<uint32_t>((uint32_t)types.size());
	for (auto &sql_type : types) {
		sql_type.Serialize(serializer);
	}
}

unique_ptr<CreateViewInfo> ViewCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateViewInfo>();
	info->schema = source.Read<string>();
	info->view_name = source.Read<string>();
	info->query = QueryNode::Deserialize(source);
	auto alias_count = source.Read<uint32_t>();
	for (uint32_t i = 0; i < alias_count; i++) {
		info->aliases.push_back(source.Read<string>());
	}
	auto type_count = source.Read<uint32_t>();
	for (uint32_t i = 0; i < type_count; i++) {
		info->types.push_back(SQLType::Deserialize(source));
	}
	return info;
}
