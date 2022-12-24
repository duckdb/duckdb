#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog.hpp"

namespace duckdb {

CreateViewInfo::CreateViewInfo() : CreateInfo(CatalogType::VIEW_ENTRY, INVALID_SCHEMA) {
}
CreateViewInfo::CreateViewInfo(string catalog_p, string schema_p, string view_name_p)
    : CreateInfo(CatalogType::VIEW_ENTRY, move(schema_p), move(catalog_p)), view_name(move(view_name_p)) {
}

CreateViewInfo::CreateViewInfo(SchemaCatalogEntry *schema, string view_name)
    : CreateViewInfo(schema->catalog->GetName(), schema->name, move(view_name)) {
}

unique_ptr<CreateInfo> CreateViewInfo::Copy() const {
	auto result = make_unique<CreateViewInfo>(catalog, schema, view_name);
	CopyProperties(*result);
	result->aliases = aliases;
	result->types = types;
	result->query = unique_ptr_cast<SQLStatement, SelectStatement>(query->Copy());
	return move(result);
}

unique_ptr<CreateViewInfo> CreateViewInfo::Deserialize(Deserializer &deserializer) {
	auto result = make_unique<CreateViewInfo>();
	result->DeserializeBase(deserializer);

	FieldReader reader(deserializer);
	result->view_name = reader.ReadRequired<string>();
	result->aliases = reader.ReadRequiredList<string>();
	result->types = reader.ReadRequiredSerializableList<LogicalType, LogicalType>();
	result->query = reader.ReadOptional<SelectStatement>(nullptr);
	reader.Finalize();

	return result;
}

void CreateViewInfo::SerializeInternal(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteString(view_name);
	writer.WriteList<string>(aliases);
	writer.WriteRegularSerializableList(types);
	writer.WriteOptional(query);
	writer.Finalize();
}

} // namespace duckdb
