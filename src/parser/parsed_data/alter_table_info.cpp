#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

void AlterInfo::Serialize(Serializer &serializer) {
	serializer.Write<AlterType>(type);
}

unique_ptr<AlterInfo> AlterInfo::Deserialize(Deserializer &source) {
	auto type = source.Read<AlterType>();
	switch (type) {
	case AlterType::ALTER_TABLE:
		return AlterTableInfo::Deserialize(source);
	case AlterType::ALTER_VIEW:
		return AlterViewInfo::Deserialize(source);
	default:
		throw SerializationException("Unknown alter type for deserialization!");
	}
}

CatalogType ChangeOwnershipInfo::GetCatalogType() {
	return entry_catalog_type;
}

unique_ptr<AlterInfo> ChangeOwnershipInfo::Copy() const {
	return make_unique_base<AlterInfo, ChangeOwnershipInfo>(entry_catalog_type, schema, name, owner_schema, owner_name);
}

void AlterTableInfo::Serialize(Serializer &serializer) {
	AlterInfo::Serialize(serializer);
	serializer.Write<AlterTableType>(alter_table_type);
	serializer.WriteString(schema);
	serializer.WriteString(name);
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
unique_ptr<AlterInfo> RenameColumnInfo::Copy() const {
	return make_unique_base<AlterInfo, RenameColumnInfo>(schema, name, old_name, new_name);
}

void RenameColumnInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(old_name);
	serializer.WriteString(new_name);
}

unique_ptr<AlterInfo> RenameColumnInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto old_name = source.Read<string>();
	auto new_name = source.Read<string>();
	return make_unique<RenameColumnInfo>(move(schema), move(table), old_name, new_name);
}

//===--------------------------------------------------------------------===//
// RenameTableInfo
//===--------------------------------------------------------------------===//
unique_ptr<AlterInfo> RenameTableInfo::Copy() const {
	return make_unique_base<AlterInfo, RenameTableInfo>(schema, name, new_table_name);
}

void RenameTableInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(new_table_name);
}

unique_ptr<AlterInfo> RenameTableInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto new_name = source.Read<string>();
	return make_unique<RenameTableInfo>(move(schema), move(table), new_name);
}

//===--------------------------------------------------------------------===//
// AddColumnInfo
//===--------------------------------------------------------------------===//
unique_ptr<AlterInfo> AddColumnInfo::Copy() const {
	return make_unique_base<AlterInfo, AddColumnInfo>(schema, name, new_column.Copy());
}

void AddColumnInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	new_column.Serialize(serializer);
}

unique_ptr<AlterInfo> AddColumnInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto new_column = ColumnDefinition::Deserialize(source);
	return make_unique<AddColumnInfo>(move(schema), move(table), move(new_column));
}

//===--------------------------------------------------------------------===//
// RemoveColumnInfo
//===--------------------------------------------------------------------===//
unique_ptr<AlterInfo> RemoveColumnInfo::Copy() const {
	return make_unique_base<AlterInfo, RemoveColumnInfo>(schema, name, removed_column, if_exists);
}

void RemoveColumnInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(removed_column);
	serializer.Write<bool>(if_exists);
}

unique_ptr<AlterInfo> RemoveColumnInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto new_name = source.Read<string>();
	auto if_exists = source.Read<bool>();
	return make_unique<RemoveColumnInfo>(move(schema), move(table), new_name, if_exists);
}

//===--------------------------------------------------------------------===//
// ChangeColumnTypeInfo
//===--------------------------------------------------------------------===//
unique_ptr<AlterInfo> ChangeColumnTypeInfo::Copy() const {
	return make_unique_base<AlterInfo, ChangeColumnTypeInfo>(schema, name, column_name, target_type,
	                                                         expression->Copy());
}

void ChangeColumnTypeInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(column_name);
	target_type.Serialize(serializer);
	serializer.WriteOptional(expression);
}

unique_ptr<AlterInfo> ChangeColumnTypeInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto column_name = source.Read<string>();
	auto target_type = LogicalType::Deserialize(source);
	auto expression = source.ReadOptional<ParsedExpression>();
	return make_unique<ChangeColumnTypeInfo>(move(schema), move(table), move(column_name), move(target_type),
	                                         move(expression));
}

//===--------------------------------------------------------------------===//
// SetDefaultInfo
//===--------------------------------------------------------------------===//
unique_ptr<AlterInfo> SetDefaultInfo::Copy() const {
	return make_unique_base<AlterInfo, SetDefaultInfo>(schema, name, column_name,
	                                                   expression ? expression->Copy() : nullptr);
}

void SetDefaultInfo::Serialize(Serializer &serializer) {
	AlterTableInfo::Serialize(serializer);
	serializer.WriteString(column_name);
	serializer.WriteOptional(expression);
}

unique_ptr<AlterInfo> SetDefaultInfo::Deserialize(Deserializer &source, string schema, string table) {
	auto column_name = source.Read<string>();
	auto new_default = source.ReadOptional<ParsedExpression>();
	return make_unique<SetDefaultInfo>(move(schema), move(table), move(column_name), move(new_default));
}

//===--------------------------------------------------------------------===//
// Alter View
//===--------------------------------------------------------------------===//
void AlterViewInfo::Serialize(Serializer &serializer) {
	AlterInfo::Serialize(serializer);
	serializer.Write<AlterViewType>(alter_view_type);
	serializer.WriteString(schema);
	serializer.WriteString(name);
}

unique_ptr<AlterInfo> AlterViewInfo::Deserialize(Deserializer &source) {
	auto type = source.Read<AlterViewType>();
	auto schema = source.Read<string>();
	auto view = source.Read<string>();
	unique_ptr<AlterViewInfo> info;
	switch (type) {
	case AlterViewType::RENAME_VIEW:
		return RenameViewInfo::Deserialize(source, schema, view);
	default:
		throw SerializationException("Unknown alter view type for deserialization!");
	}
}

//===--------------------------------------------------------------------===//
// RenameViewInfo
//===--------------------------------------------------------------------===//
unique_ptr<AlterInfo> RenameViewInfo::Copy() const {
	return make_unique_base<AlterInfo, RenameViewInfo>(schema, name, new_view_name);
}

void RenameViewInfo::Serialize(Serializer &serializer) {
	AlterViewInfo::Serialize(serializer);
	serializer.WriteString(new_view_name);
}

unique_ptr<AlterInfo> RenameViewInfo::Deserialize(Deserializer &source, string schema, string view) {
	auto new_name = source.Read<string>();
	return make_unique<RenameViewInfo>(move(schema), move(view), new_name);
}
} // namespace duckdb
