#include "duckdb/catalog/catalog_entry/type_catalog_entry.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/serializer.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"

#include <algorithm>
#include <sstream>

namespace duckdb {

TypeCatalogEntry::TypeCatalogEntry(Catalog *catalog, SchemaCatalogEntry *schema, CreateTypeInfo *info)
    : StandardEntry(CatalogType::TYPE_ENTRY, schema, catalog, info->name) {
	user_type = make_unique<LogicalType>(*info->type);
}

void TypeCatalogEntry::Serialize(Serializer &serializer) {
	serializer.WriteString(schema->name);
	serializer.WriteString(name);
	user_type->Serialize(serializer);
}

unique_ptr<CreateTypeInfo> TypeCatalogEntry::Deserialize(Deserializer &source) {
	auto info = make_unique<CreateTypeInfo>();
	info->schema = source.Read<string>();
	info->name = source.Read<string>();
	info->type = make_unique<LogicalType>(LogicalType::Deserialize(source));
	return info;
}

string TypeCatalogEntry::ToSQL() {
	std::stringstream ss;
	switch (user_type->id()) {
	case (LogicalTypeId::ENUM): {
		Vector values_insert_order(EnumType::GetValuesInsertOrder(*user_type));
		idx_t size = EnumType::GetSize(*user_type);
		ss << "CREATE TYPE ";
		ss << name;
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
