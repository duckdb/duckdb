#include "duckdb/parser/parsed_data/drop_info.hpp"

namespace duckdb {

DropInfo::DropInfo() : ParseInfo(TYPE), catalog(INVALID_CATALOG), schema(INVALID_SCHEMA), cascade(false) {
}

unique_ptr<DropInfo> DropInfo::Copy() const {
	auto result = make_uniq<DropInfo>();
	result->type = type;
	result->catalog = catalog;
	result->schema = schema;
	result->name = name;
	result->if_not_found = if_not_found;
	result->cascade = cascade;
	result->allow_drop_internal = allow_drop_internal;
	return result;
}

void DropInfo::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<CatalogType>(type);
	writer.WriteString(catalog);
	writer.WriteString(schema);
	writer.WriteString(name);
	writer.WriteField(if_not_found);
	writer.WriteField(cascade);
	writer.WriteField(allow_drop_internal);
	writer.Finalize();
}

unique_ptr<ParseInfo> DropInfo::Deserialize(Deserializer &deserializer) {
	FieldReader reader(deserializer);
	auto drop_info = make_uniq<DropInfo>();
	drop_info->type = reader.ReadRequired<CatalogType>();
	drop_info->catalog = reader.ReadRequired<string>();
	drop_info->schema = reader.ReadRequired<string>();
	drop_info->name = reader.ReadRequired<string>();
	drop_info->if_not_found = reader.ReadRequired<OnEntryNotFound>();
	drop_info->cascade = reader.ReadRequired<bool>();
	drop_info->allow_drop_internal = reader.ReadRequired<bool>();
	reader.Finalize();
	return std::move(drop_info);
}

} // namespace duckdb
