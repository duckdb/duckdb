#include "duckdb/parser/parsed_data/alter_table_info.hpp"

#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ChangeOwnershipInfo
//===--------------------------------------------------------------------===//
ChangeOwnershipInfo::ChangeOwnershipInfo(CatalogType entry_catalog_type, string entry_catalog_p, string entry_schema_p,
                                         string entry_name_p, string owner_schema_p, string owner_name_p,
                                         bool if_exists)
    : AlterInfo(AlterType::CHANGE_OWNERSHIP, move(entry_catalog_p), move(entry_schema_p), move(entry_name_p),
                if_exists),
      entry_catalog_type(entry_catalog_type), owner_schema(move(owner_schema_p)), owner_name(move(owner_name_p)) {
}

CatalogType ChangeOwnershipInfo::GetCatalogType() const {
	return entry_catalog_type;
}

unique_ptr<AlterInfo> ChangeOwnershipInfo::Copy() const {
	return make_unique_base<AlterInfo, ChangeOwnershipInfo>(entry_catalog_type, catalog, schema, name, owner_schema,
	                                                        owner_name, if_exists);
}

void ChangeOwnershipInfo::Serialize(FieldWriter &writer) const {
	throw InternalException("ChangeOwnershipInfo cannot be serialized");
}

//===--------------------------------------------------------------------===//
// AlterTableInfo
//===--------------------------------------------------------------------===//
AlterTableInfo::AlterTableInfo(AlterTableType type, AlterEntryData data)
    : AlterInfo(AlterType::ALTER_TABLE, move(data.catalog), move(data.schema), move(data.name), data.if_exists),
      alter_table_type(type) {
}
AlterTableInfo::~AlterTableInfo() {
}

CatalogType AlterTableInfo::GetCatalogType() const {
	return CatalogType::TABLE_ENTRY;
}

void AlterTableInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<AlterTableType>(alter_table_type);
	writer.WriteString(catalog);
	writer.WriteString(schema);
	writer.WriteString(name);
	writer.WriteField(if_exists);
	SerializeAlterTable(writer);
}

unique_ptr<AlterInfo> AlterTableInfo::Deserialize(FieldReader &reader) {
	auto type = reader.ReadRequired<AlterTableType>();
	AlterEntryData data;
	data.catalog = reader.ReadRequired<string>();
	data.schema = reader.ReadRequired<string>();
	data.name = reader.ReadRequired<string>();
	data.if_exists = reader.ReadRequired<bool>();

	unique_ptr<AlterTableInfo> info;
	switch (type) {
	case AlterTableType::RENAME_COLUMN:
		return RenameColumnInfo::Deserialize(reader, move(data));
	case AlterTableType::RENAME_TABLE:
		return RenameTableInfo::Deserialize(reader, move(data));
	case AlterTableType::ADD_COLUMN:
		return AddColumnInfo::Deserialize(reader, move(data));
	case AlterTableType::REMOVE_COLUMN:
		return RemoveColumnInfo::Deserialize(reader, move(data));
	case AlterTableType::ALTER_COLUMN_TYPE:
		return ChangeColumnTypeInfo::Deserialize(reader, move(data));
	case AlterTableType::SET_DEFAULT:
		return SetDefaultInfo::Deserialize(reader, move(data));
	case AlterTableType::FOREIGN_KEY_CONSTRAINT:
		return AlterForeignKeyInfo::Deserialize(reader, move(data));
	case AlterTableType::SET_NOT_NULL:
		return SetNotNullInfo::Deserialize(reader, move(data));
	case AlterTableType::DROP_NOT_NULL:
		return DropNotNullInfo::Deserialize(reader, move(data));
	default:
		throw SerializationException("Unknown alter table type for deserialization!");
	}
}

//===--------------------------------------------------------------------===//
// RenameColumnInfo
//===--------------------------------------------------------------------===//
RenameColumnInfo::RenameColumnInfo(AlterEntryData data, string old_name_p, string new_name_p)
    : AlterTableInfo(AlterTableType::RENAME_COLUMN, move(data)), old_name(move(old_name_p)),
      new_name(move(new_name_p)) {
}
RenameColumnInfo::~RenameColumnInfo() {
}

unique_ptr<AlterInfo> RenameColumnInfo::Copy() const {
	return make_unique_base<AlterInfo, RenameColumnInfo>(GetAlterEntryData(), old_name, new_name);
}

void RenameColumnInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(old_name);
	writer.WriteString(new_name);
}

unique_ptr<AlterInfo> RenameColumnInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto old_name = reader.ReadRequired<string>();
	auto new_name = reader.ReadRequired<string>();
	return make_unique<RenameColumnInfo>(move(data), old_name, new_name);
}

//===--------------------------------------------------------------------===//
// RenameTableInfo
//===--------------------------------------------------------------------===//
RenameTableInfo::RenameTableInfo(AlterEntryData data, string new_name_p)
    : AlterTableInfo(AlterTableType::RENAME_TABLE, move(data)), new_table_name(move(new_name_p)) {
}
RenameTableInfo::~RenameTableInfo() {
}

unique_ptr<AlterInfo> RenameTableInfo::Copy() const {
	return make_unique_base<AlterInfo, RenameTableInfo>(GetAlterEntryData(), new_table_name);
}

void RenameTableInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(new_table_name);
}

unique_ptr<AlterInfo> RenameTableInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto new_name = reader.ReadRequired<string>();
	return make_unique<RenameTableInfo>(move(data), new_name);
}

//===--------------------------------------------------------------------===//
// AddColumnInfo
//===--------------------------------------------------------------------===//
AddColumnInfo::AddColumnInfo(AlterEntryData data, ColumnDefinition new_column, bool if_column_not_exists)
    : AlterTableInfo(AlterTableType::ADD_COLUMN, move(data)), new_column(move(new_column)),
      if_column_not_exists(if_column_not_exists) {
}

AddColumnInfo::~AddColumnInfo() {
}

unique_ptr<AlterInfo> AddColumnInfo::Copy() const {
	return make_unique_base<AlterInfo, AddColumnInfo>(GetAlterEntryData(), new_column.Copy(), if_column_not_exists);
}

void AddColumnInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteSerializable(new_column);
	writer.WriteField<bool>(if_column_not_exists);
}

unique_ptr<AlterInfo> AddColumnInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto new_column = reader.ReadRequiredSerializable<ColumnDefinition, ColumnDefinition>();
	auto if_column_not_exists = reader.ReadRequired<bool>();
	return make_unique<AddColumnInfo>(move(data), move(new_column), if_column_not_exists);
}

//===--------------------------------------------------------------------===//
// RemoveColumnInfo
//===--------------------------------------------------------------------===//
RemoveColumnInfo::RemoveColumnInfo(AlterEntryData data, string removed_column, bool if_column_exists, bool cascade)
    : AlterTableInfo(AlterTableType::REMOVE_COLUMN, move(data)), removed_column(move(removed_column)),
      if_column_exists(if_column_exists), cascade(cascade) {
}
RemoveColumnInfo::~RemoveColumnInfo() {
}

unique_ptr<AlterInfo> RemoveColumnInfo::Copy() const {
	return make_unique_base<AlterInfo, RemoveColumnInfo>(GetAlterEntryData(), removed_column, if_column_exists,
	                                                     cascade);
}

void RemoveColumnInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(removed_column);
	writer.WriteField<bool>(if_column_exists);
	writer.WriteField<bool>(cascade);
}

unique_ptr<AlterInfo> RemoveColumnInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto new_name = reader.ReadRequired<string>();
	auto if_column_exists = reader.ReadRequired<bool>();
	auto cascade = reader.ReadRequired<bool>();
	return make_unique<RemoveColumnInfo>(move(data), move(new_name), if_column_exists, cascade);
}

//===--------------------------------------------------------------------===//
// ChangeColumnTypeInfo
//===--------------------------------------------------------------------===//
ChangeColumnTypeInfo::ChangeColumnTypeInfo(AlterEntryData data, string column_name, LogicalType target_type,
                                           unique_ptr<ParsedExpression> expression)
    : AlterTableInfo(AlterTableType::ALTER_COLUMN_TYPE, move(data)), column_name(move(column_name)),
      target_type(move(target_type)), expression(move(expression)) {
}
ChangeColumnTypeInfo::~ChangeColumnTypeInfo() {
}

unique_ptr<AlterInfo> ChangeColumnTypeInfo::Copy() const {
	return make_unique_base<AlterInfo, ChangeColumnTypeInfo>(GetAlterEntryData(), column_name, target_type,
	                                                         expression->Copy());
}

void ChangeColumnTypeInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(column_name);
	writer.WriteSerializable(target_type);
	writer.WriteOptional(expression);
}

unique_ptr<AlterInfo> ChangeColumnTypeInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto column_name = reader.ReadRequired<string>();
	auto target_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto expression = reader.ReadOptional<ParsedExpression>(nullptr);
	return make_unique<ChangeColumnTypeInfo>(move(data), move(column_name), move(target_type), move(expression));
}

//===--------------------------------------------------------------------===//
// SetDefaultInfo
//===--------------------------------------------------------------------===//
SetDefaultInfo::SetDefaultInfo(AlterEntryData data, string column_name_p, unique_ptr<ParsedExpression> new_default)
    : AlterTableInfo(AlterTableType::SET_DEFAULT, move(data)), column_name(move(column_name_p)),
      expression(move(new_default)) {
}
SetDefaultInfo::~SetDefaultInfo() {
}

unique_ptr<AlterInfo> SetDefaultInfo::Copy() const {
	return make_unique_base<AlterInfo, SetDefaultInfo>(GetAlterEntryData(), column_name,
	                                                   expression ? expression->Copy() : nullptr);
}

void SetDefaultInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(column_name);
	writer.WriteOptional(expression);
}

unique_ptr<AlterInfo> SetDefaultInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto column_name = reader.ReadRequired<string>();
	auto new_default = reader.ReadOptional<ParsedExpression>(nullptr);
	return make_unique<SetDefaultInfo>(move(data), move(column_name), move(new_default));
}

//===--------------------------------------------------------------------===//
// SetNotNullInfo
//===--------------------------------------------------------------------===//
SetNotNullInfo::SetNotNullInfo(AlterEntryData data, string column_name_p)
    : AlterTableInfo(AlterTableType::SET_NOT_NULL, move(data)), column_name(move(column_name_p)) {
}
SetNotNullInfo::~SetNotNullInfo() {
}

unique_ptr<AlterInfo> SetNotNullInfo::Copy() const {
	return make_unique_base<AlterInfo, SetNotNullInfo>(GetAlterEntryData(), column_name);
}

void SetNotNullInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(column_name);
}

unique_ptr<AlterInfo> SetNotNullInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto column_name = reader.ReadRequired<string>();
	return make_unique<SetNotNullInfo>(move(data), move(column_name));
}

//===--------------------------------------------------------------------===//
// DropNotNullInfo
//===--------------------------------------------------------------------===//
DropNotNullInfo::DropNotNullInfo(AlterEntryData data, string column_name_p)
    : AlterTableInfo(AlterTableType::DROP_NOT_NULL, move(data)), column_name(move(column_name_p)) {
}
DropNotNullInfo::~DropNotNullInfo() {
}

unique_ptr<AlterInfo> DropNotNullInfo::Copy() const {
	return make_unique_base<AlterInfo, DropNotNullInfo>(GetAlterEntryData(), column_name);
}

void DropNotNullInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(column_name);
}

unique_ptr<AlterInfo> DropNotNullInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto column_name = reader.ReadRequired<string>();
	return make_unique<DropNotNullInfo>(move(data), move(column_name));
}

//===--------------------------------------------------------------------===//
// AlterForeignKeyInfo
//===--------------------------------------------------------------------===//
AlterForeignKeyInfo::AlterForeignKeyInfo(AlterEntryData data, string fk_table, vector<string> pk_columns,
                                         vector<string> fk_columns, vector<PhysicalIndex> pk_keys,
                                         vector<PhysicalIndex> fk_keys, AlterForeignKeyType type_p)
    : AlterTableInfo(AlterTableType::FOREIGN_KEY_CONSTRAINT, move(data)), fk_table(move(fk_table)),
      pk_columns(move(pk_columns)), fk_columns(move(fk_columns)), pk_keys(move(pk_keys)), fk_keys(move(fk_keys)),
      type(type_p) {
}
AlterForeignKeyInfo::~AlterForeignKeyInfo() {
}

unique_ptr<AlterInfo> AlterForeignKeyInfo::Copy() const {
	return make_unique_base<AlterInfo, AlterForeignKeyInfo>(GetAlterEntryData(), fk_table, pk_columns, fk_columns,
	                                                        pk_keys, fk_keys, type);
}

void AlterForeignKeyInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(fk_table);
	writer.WriteList<string>(pk_columns);
	writer.WriteList<string>(fk_columns);
	writer.WriteIndexList<PhysicalIndex>(pk_keys);
	writer.WriteIndexList<PhysicalIndex>(fk_keys);
	writer.WriteField<AlterForeignKeyType>(type);
}

unique_ptr<AlterInfo> AlterForeignKeyInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto fk_table = reader.ReadRequired<string>();
	auto pk_columns = reader.ReadRequiredList<string>();
	auto fk_columns = reader.ReadRequiredList<string>();
	auto pk_keys = reader.ReadRequiredIndexList<PhysicalIndex>();
	auto fk_keys = reader.ReadRequiredIndexList<PhysicalIndex>();
	auto type = reader.ReadRequired<AlterForeignKeyType>();
	return make_unique<AlterForeignKeyInfo>(move(data), move(fk_table), move(pk_columns), move(fk_columns),
	                                        move(pk_keys), move(fk_keys), type);
}

//===--------------------------------------------------------------------===//
// Alter View
//===--------------------------------------------------------------------===//
AlterViewInfo::AlterViewInfo(AlterViewType type, AlterEntryData data)
    : AlterInfo(AlterType::ALTER_VIEW, move(data.catalog), move(data.schema), move(data.name), data.if_exists),
      alter_view_type(type) {
}
AlterViewInfo::~AlterViewInfo() {
}

CatalogType AlterViewInfo::GetCatalogType() const {
	return CatalogType::VIEW_ENTRY;
}

void AlterViewInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<AlterViewType>(alter_view_type);
	writer.WriteString(catalog);
	writer.WriteString(schema);
	writer.WriteString(name);
	writer.WriteField<bool>(if_exists);
	SerializeAlterView(writer);
}

unique_ptr<AlterInfo> AlterViewInfo::Deserialize(FieldReader &reader) {
	auto type = reader.ReadRequired<AlterViewType>();
	AlterEntryData data;
	data.catalog = reader.ReadRequired<string>();
	data.schema = reader.ReadRequired<string>();
	data.name = reader.ReadRequired<string>();
	data.if_exists = reader.ReadRequired<bool>();
	unique_ptr<AlterViewInfo> info;
	switch (type) {
	case AlterViewType::RENAME_VIEW:
		return RenameViewInfo::Deserialize(reader, move(data));
	default:
		throw SerializationException("Unknown alter view type for deserialization!");
	}
}

//===--------------------------------------------------------------------===//
// RenameViewInfo
//===--------------------------------------------------------------------===//
RenameViewInfo::RenameViewInfo(AlterEntryData data, string new_name_p)
    : AlterViewInfo(AlterViewType::RENAME_VIEW, move(data)), new_view_name(move(new_name_p)) {
}
RenameViewInfo::~RenameViewInfo() {
}

unique_ptr<AlterInfo> RenameViewInfo::Copy() const {
	return make_unique_base<AlterInfo, RenameViewInfo>(GetAlterEntryData(), new_view_name);
}

void RenameViewInfo::SerializeAlterView(FieldWriter &writer) const {
	writer.WriteString(new_view_name);
}

unique_ptr<AlterInfo> RenameViewInfo::Deserialize(FieldReader &reader, AlterEntryData data) {
	auto new_name = reader.ReadRequired<string>();
	return make_unique<RenameViewInfo>(move(data), new_name);
}
} // namespace duckdb
