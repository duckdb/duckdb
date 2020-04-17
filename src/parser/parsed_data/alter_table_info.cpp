#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/common/serializer.hpp"

using namespace duckdb;
using namespace std;

void AlterInfo::Serialize(Serializer &serializer) {
	serializer.Write<AlterType>(type);
}

unique_ptr<AlterInfo> AlterInfo::Deserialize(Deserializer &source) {
	auto type = source.Read<AlterType>();
	switch (type) {
	case AlterType::ALTER_TABLE:
		return AlterTableInfo::Deserialize(source);
	default:
		throw SerializationException("Unknown alter type for deserialization!");
	}
}

void AlterTableInfo::Serialize(Serializer &serializer) {
	AlterInfo::Serialize(serializer);
	serializer.Write<AlterTableType>(alter_table_type);
	serializer.WriteString(schema);
	serializer.WriteString(table);
}

unique_ptr<AlterInfo> AlterTableInfo::Deserialize(Deserializer &source) {
	auto type = source.Read<AlterTableType>();
	auto schema = source.Read<string>();
	auto table = source.Read<string>();
	unique_ptr<AlterTableInfo> info;
	switch (type) {
	case AlterTableType::RENAME_COLUMN:
		return RenameColumnInfo::Deserialize(source, schema, table);
    case AlterTableType::RENAME_TABLE:
        return RenameTableInfo::Deserialize(source, schema, table);
	default:
		throw SerializationException("Unknown alter table type for deserialization!");
	}
}

void RenameColumnInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(name);
	serializer.WriteString(new_name);
}

unique_ptr<AlterInfo> RenameColumnInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto name = source.Read<string>();
	auto new_name = source.Read<string>();
	return make_unique<RenameColumnInfo>(schema, table, name, new_name);
}

void RenameTableInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
    serializer.WriteString(new_table_name);
}

unique_ptr<AlterInfo> RenameTableInfo::Deserialize(Deserializer &source, string schema, string table) {
    auto new_name = source.Read<string>();
	return make_unique<RenameTableInfo>(schema, table, new_name);
}
