#include "duckdb/parser/parsed_data/create_info.hpp"

#include "duckdb/parser/parsed_data/create_index_info.hpp"
#include "duckdb/parser/parsed_data/create_schema_info.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/parsed_data/create_view_info.hpp"
#include "duckdb/parser/parsed_data/create_sequence_info.hpp"
#include "duckdb/parser/parsed_data/create_type_info.hpp"
#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/parsed_data/create_macro_info.hpp"

namespace duckdb {

void CreateInfo::Serialize(Serializer &serializer) const {
	serializer.Write(type);
	serializer.WriteString(catalog);
	serializer.WriteString(schema);
	serializer.Write(on_conflict);
	serializer.Write(temporary);
	serializer.Write(internal);
	serializer.WriteString(sql);
	SerializeInternal(serializer);
}

void CreateInfo::DeserializeBase(Deserializer &deserializer) {
	this->catalog = deserializer.Read<string>();
	this->schema = deserializer.Read<string>();
	this->on_conflict = deserializer.Read<OnCreateConflict>();
	this->temporary = deserializer.Read<bool>();
	this->internal = deserializer.Read<bool>();
	this->sql = deserializer.Read<string>();
}

unique_ptr<CreateInfo> CreateInfo::Deserialize(Deserializer &deserializer) {
	auto type = deserializer.Read<CatalogType>();
	switch (type) {
	case CatalogType::INDEX_ENTRY:
		return CreateIndexInfo::Deserialize(deserializer);
	case CatalogType::TABLE_ENTRY:
		return CreateTableInfo::Deserialize(deserializer);
	case CatalogType::SCHEMA_ENTRY:
		return CreateSchemaInfo::Deserialize(deserializer);
	case CatalogType::VIEW_ENTRY:
		return CreateViewInfo::Deserialize(deserializer);
	case CatalogType::TYPE_ENTRY:
		return CreateTypeInfo::Deserialize(deserializer);
	case CatalogType::MACRO_ENTRY:
	case CatalogType::TABLE_MACRO_ENTRY:
		return CreateMacroInfo::Deserialize(deserializer);
	case CatalogType::SEQUENCE_ENTRY:
		return CreateSequenceInfo::Deserialize(deserializer);
	default:
		throw NotImplementedException("Cannot deserialize '%s'", CatalogTypeToString(type));
	}
}

unique_ptr<CreateInfo> CreateInfo::Deserialize(Deserializer &source, PlanDeserializationState &state) {
	return Deserialize(source);
}

void CreateInfo::CopyProperties(CreateInfo &other) const {
	other.type = type;
	other.catalog = catalog;
	other.schema = schema;
	other.on_conflict = on_conflict;
	other.temporary = temporary;
	other.internal = internal;
	other.sql = sql;
}

unique_ptr<AlterInfo> CreateInfo::GetAlterInfo() const {
	throw NotImplementedException("GetAlterInfo not implemented for this type");
}

} // namespace duckdb
