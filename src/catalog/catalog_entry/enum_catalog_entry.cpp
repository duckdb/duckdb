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
	if (info->values.size() > NumericLimits<uint16_t>::Maximum()) {
		throw NotImplementedException("We only support up to 65,535 values for ENUMs");
	}
	idx_t counter = 0;
	for (auto &value : info->values) {
		if (values.find(value) != values.end()) {
			throw std::runtime_error("Duplicate value violates ENUM's unique constraint");
		}
		values[value] = counter++;
		values_insert_order.push_back(value);
	}
	this->temporary = info->temporary;
}

void EnumCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	serializer.Write<vector<string>>(values_insert_order);
}

unique_ptr<CreateEnumInfo> EnumCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateEnumInfo>();
	info->schema = source.Read<string>();
	info->name = source.Read<string>();
	info->values = source.Read<vector<string>>();
	return info;
}

string EnumCatalogEntry::ToSQL() {
	std::stringstream ss;
	ss << "CREATE TYPE ";
	ss << name;
	ss << " AS ENUM ( ";
	vector<string> string_values(values.size());
	for (auto &value : values) {
		string_values[value.second] = value.first;
	}
	for (idx_t i = 0; i < string_values.size(); i++) {
		ss << "'" << string_values[i] << "'";
		if (i != string_values.size() - 1) {
			ss << ", ";
		}
	}
	ss << ");";
	return ss.str();
}

} // namespace duckdb
