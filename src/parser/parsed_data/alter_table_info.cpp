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
	case AlterTableType::ADD_COLUMN:
		return AddColumnInfo::Deserialize(source, schema, table);
	case AlterTableType::REMOVE_COLUMN:
		return RemoveColumnInfo::Deserialize(source, schema, table);
	case AlterTableType::ALTER_COLUMN_TYPE:
		return ChangeColumnTypeInfo::Deserialize(source, schema, table);
	case AlterTableType::SET_DEFAULT:
		return SetDefaultInfo::Deserialize(source, schema, table);
	default:
		throw SerializationException("Unknown alter table type for deserialization!");
	}
}

//===--------------------------------------------------------------------===//
// RenameColumnInfo
//===--------------------------------------------------------------------===//
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

//===--------------------------------------------------------------------===//
// RenameTableInfo
//===--------------------------------------------------------------------===//
void RenameTableInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(new_table_name);
}

unique_ptr<AlterInfo> RenameTableInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto new_name = source.Read<string>();
	return make_unique<RenameTableInfo>(schema, table, new_name);
}

//===--------------------------------------------------------------------===//
// AddColumnInfo
//===--------------------------------------------------------------------===//
void AddColumnInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	new_column.Serialize(serializer);
}

unique_ptr<AlterInfo> AddColumnInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto new_column = ColumnDefinition::Deserialize(source);
	return make_unique<AddColumnInfo>(schema, table, move(new_column));
}

//===--------------------------------------------------------------------===//
// RemoveColumnInfo
//===--------------------------------------------------------------------===//
void RemoveColumnInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(removed_column);
	serializer.Write<bool>(if_exists);
}

unique_ptr<AlterInfo> RemoveColumnInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto new_name = source.Read<string>();
	auto if_exists = source.Read<bool>();
	return make_unique<RemoveColumnInfo>(schema, table, new_name, if_exists);
}

//===--------------------------------------------------------------------===//
// ChangeColumnTypeInfo
//===--------------------------------------------------------------------===//
void ChangeColumnTypeInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(column_name);
	target_type.Serialize(serializer);
	serializer.WriteOptional(expression);
}

unique_ptr<AlterInfo> ChangeColumnTypeInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto column_name = source.Read<string>();
	auto target_type = SQLType::Deserialize(source);
	auto expression = source.ReadOptional<ParsedExpression>();
	return make_unique<ChangeColumnTypeInfo>(schema, table, move(column_name), move(target_type), move(expression));
}

//===--------------------------------------------------------------------===//
// SetDefaultInfo
//===--------------------------------------------------------------------===//
void SetDefaultInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(column_name);
	serializer.WriteOptional(expression);
}

unique_ptr<AlterInfo> SetDefaultInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto column_name = source.Read<string>();
	auto new_default = source.ReadOptional<ParsedExpression>();
	return make_unique<SetDefaultInfo>(schema, table, move(column_name), move(new_default));
}
