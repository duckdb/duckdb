#include "duckdb/catalog/catalog_entry/enum_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/common/limits.hpp"
#include <algorithm>
#include <sstream>

namespace duckdb {

EnumCatalogEntry::EnumCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateEnumInfo *info)
    : StandardEntry(CatalogType::ENUM_ENTRY, schema, catalog, info->name) {
	if (info->values.size() > NumericLimits<uint32_t>::Maximum()) {
		throw NotImplementedException("We only support up to 4,294,967,295 values for ENUMs");
	}
	unordered_set<string> values;
	for (auto &value : info->values) {
		if (values.find(value) != values.end()) {
			throw BinderException("Duplicate value violates ENUM's unique constraint");
		}
		values.insert(value);
		values_insert_order.push_back(value);
	}
	this->temporary = info->temporary;
}

void EnumCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	serializer.WriteStringVector(values_insert_order);
}

unique_ptr<CreateEnumInfo> EnumCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateEnumInfo>();
	info->schema = source.Read<string>();
	info->name = source.Read<string>();
	source.ReadStringVector(info->values);
	return info;
}

string EnumCatalogEntry::ToSQL() {
	std::stringstream ss;
	ss << "CREATE TYPE ";
	ss << name;
	ss << " AS ENUM ( ";

	for (idx_t i = 0; i < values_insert_order.size(); i++) {
		ss << "'" << values_insert_order[i] << "'";
		if (i != values_insert_order.size() - 1) {
			ss << ", ";
		}
	}
	ss << ");";
	return ss.str();
}

} // namespace duckdb
