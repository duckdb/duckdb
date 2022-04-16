#include "duckdb/catalog/catalog_entry/custom_type_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/create_custom_type_info.hpp"
#include "duckdb/common/types/vector.hpp"
#include <algorithm>
#include <sstream>
#include <iostream>

namespace duckdb {

CustomTypeCatalogEntry::CustomTypeCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateCustomTypeInfo *info)
    : StandardEntry(CatalogType::TYPE_CUSTOM_ENTRY, schema, catalog, info->name), user_type(info->type) {
}

void CustomTypeCatalogEntry::Serialize(Serializer &serializer) {
	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteSerializable(user_type);
	writer.Finalize();
}

unique_ptr<CreateCustomTypeInfo> CustomTypeCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateCustomTypeInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	info->type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	reader.Finalize();

	return info;
}

string CustomTypeCatalogEntry::ToSQL() {
	std::stringstream ss;
	ss << "CREATE TYPE ";
	ss << KeywordHelper::WriteOptionallyQuoted(name);
	ss << " (";
	auto parameters = CustomType::GetParameters(user_type);
	idx_t i = 0;
	for (auto const &iter : parameters) {
		ss << (i == 0 ? "" : ", ") << TransformCustomTypeParameterToString(iter.first) << " := '" << iter.second << "'";
		i++;
	}
	ss << ");";

	return ss.str();
}

} // namespace duckdb