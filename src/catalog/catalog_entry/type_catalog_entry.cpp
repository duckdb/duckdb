#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/keyword_helper.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/common/types/vector.hpp"
#include <algorithm>
#include <sstream>

namespace duckdb {

TypeCatalogEntry::TypeCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTypeInfo *info)
    : StandardEntry(CatalogType::TYPE_ENTRY, schema, catalog, info->name), user_type(info->type) {
	this->temporary = info->temporary;
	this->internal = info->internal;
}

void TypeCatalogEntry::Serialize(Serializer &serializer) {
	D_ASSERT(!internal);
	FieldWriter writer(serializer);
	writer.WriteString(schema->name);
	writer.WriteString(name);
	writer.WriteSerializable(user_type);
	writer.Finalize();
}

unique_ptr<CreateTypeInfo> TypeCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateTypeInfo>();

	FieldReader reader(source);
	info->schema = reader.ReadRequired<string>();
	info->name = reader.ReadRequired<string>();
	info->type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	reader.Finalize();

	return info;
}

string TypeCatalogEntry::ToSQL() {
	std::stringstream ss;
	switch (user_type.id()) {
	case (LogicalTypeId::ENUM): {
		Vector values_insert_order(EnumType::GetValuesInsertOrder(user_type));
		idx_t size = EnumType::GetSize(user_type);
		ss << "CREATE TYPE ";
		ss << KeywordHelper::WriteOptionallyQuoted(name);
		ss << " AS ENUM ( ";

		for (idx_t i = 0; i < size; i++) {
			ss << "'" << values_insert_order.GetValue(i).ToString() << "'";
			if (i != size - 1) {
				ss << ", ";
			}
		}
		ss << ");";
		break;
	}
	default:
		throw InternalException("Logical Type can't be used as a User Defined Type");
	}

	return ss.str();
}

} // namespace duckdb
