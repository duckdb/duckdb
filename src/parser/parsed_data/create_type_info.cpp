#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreateTypeInfo::CreateTypeInfo() : CreateInfo(CatalogType::TYPE_ENTRY) {
}
CreateTypeInfo::CreateTypeInfo(string name_p, LogicalType type_p)
    : CreateInfo(CatalogType::TYPE_ENTRY), name(std::move(name_p)), type(std::move(type_p)) {
}

unique_ptr<CreateInfo> CreateTypeInfo::Copy() const {
	auto result = make_uniq<CreateTypeInfo>();
	CopyProperties(*result);
	result->name = name;
	result->type = type;
	if (query) {
		result->query = query->Copy();
	}
	return std::move(result);
}

void CreateTypeInfo::SerializeInternal(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(name);
	writer.WriteSerializable(type);
	if (query) {
		throw InternalException("Cannot serialize CreateTypeInfo with query");
	}
	writer.Finalize();
}

unique_ptr<CreateTypeInfo> CreateTypeInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_uniq<CreateTypeInfo>();
	result->DeserializeBase(deserializer);

	FieldReader reader(deserializer);
	result->name = reader.ReadRequired<string>();
	result->type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	reader.Finalize();

	return result;
}

} // namespace duckdb
