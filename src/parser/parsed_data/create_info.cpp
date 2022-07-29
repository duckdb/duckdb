#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {
void CreateInfo::DeserializeBase(Deserializer &deserializer) {
	this->schema = deserializer.Read<string>();
	this->on_conflict = deserializer.Read<OnCreateConflict>();
	this->temporary = deserializer.Read<bool>();
	this->internal = deserializer.Read<bool>();
	this->sql = deserializer.Read<string>();
}

void CreateInfo::Serialize(Serializer &serializer) const {
	serializer.Write(type);
	serializer.WriteString(schema);
	serializer.Write(on_conflict);
	serializer.Write(temporary);
	serializer.Write(internal);
	serializer.WriteString(sql);
	SerializeChild(serializer);
}

unique_ptr<CreateInfo> CreateInfo::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.Read<CatalogType>();
	switch (type) {
	case CatalogType::TABLE_ENTRY:
		return CreateTableInfo::Deserialize(deserializer);
	default:
		throw NotImplementedException("Cannot deserialize '%s'", CatalogTypeToString(type));
	}
}

void CreateInfo::CopyProperties(CreateInfo &other) const {
	other.type = type;
	other.schema = schema;
	other.on_conflict = on_conflict;
	other.temporary = temporary;
	other.internal = internal;
	other.sql = sql;
}
} // namespace duckdb
