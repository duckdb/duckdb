#include "duckdb/parser/parsed_data/alter_table_info.hpp"
#include "duckdb/common/field_writer.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {

AlterInfo::AlterInfo(AlterType type, string schema_p, string name_p)
    : type(type), if_exists(false), schema(move(schema_p)), name(move(name_p)) {
}

AlterInfo::~AlterInfo() {
}

void AlterInfo::Serialize(Serializer &serializer) const {
	FieldWriter writer(serializer);
	writer.WriteField<AlterType>(type);
	Serialize(writer);
	writer.Finalize();
}

unique_ptr<AlterInfo> AlterInfo::Deserialize(Deserializer &source) {
	FieldReader reader(source);
	auto type = reader.ReadRequired<AlterType>();

	unique_ptr<AlterInfo> result;
	switch (type) {
	case AlterType::ALTER_TABLE:
		result = AlterTableInfo::Deserialize(reader);
		break;
	case AlterType::ALTER_VIEW:
		result = AlterViewInfo::Deserialize(reader);
		break;
	default:
		throw SerializationException("Unknown alter type for deserialization!");
	}
	reader.Finalize();

	return result;
}

//===--------------------------------------------------------------------===//
// ChangeOwnershipInfo
//===--------------------------------------------------------------------===//
ChangeOwnershipInfo::ChangeOwnershipInfo(CatalogType entry_catalog_type, string entry_schema_p, string entry_name_p,
                                         string owner_schema_p, string owner_name_p)
    : AlterInfo(AlterType::CHANGE_OWNERSHIP, move(entry_schema_p), move(entry_name_p)),
      entry_catalog_type(entry_catalog_type), owner_schema(move(owner_schema_p)), owner_name(move(owner_name_p)) {
}

CatalogType ChangeOwnershipInfo::GetCatalogType() const {
	return entry_catalog_type;
}

unique_ptr<AlterInfo> ChangeOwnershipInfo::Copy() const {
	return make_unique_base<AlterInfo, ChangeOwnershipInfo>(entry_catalog_type, schema, name, owner_schema, owner_name);
}

void ChangeOwnershipInfo::Serialize(FieldWriter &writer) const {
	throw InternalException("ChangeOwnershipInfo cannot be serialized");
}

//===--------------------------------------------------------------------===//
// AlterTableInfo
//===--------------------------------------------------------------------===//
AlterTableInfo::AlterTableInfo(AlterTableType type, string schema_p, string table_p)
    : AlterInfo(AlterType::ALTER_TABLE, move(move(schema_p)), move(table_p)), alter_table_type(type) {
}
AlterTableInfo::~AlterTableInfo() {
}

CatalogType AlterTableInfo::GetCatalogType() const {
	return CatalogType::TABLE_ENTRY;
}

void AlterTableInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<AlterTableType>(alter_table_type);
	writer.WriteString(schema);
	writer.WriteString(name);
	SerializeAlterTable(writer);
}

unique_ptr<AlterInfo> AlterTableInfo::Deserialize(FieldReader &reader) {
	auto type = reader.ReadRequired<AlterTableType>();
	auto schema = reader.ReadRequired<string>();
	auto table = reader.ReadRequired<string>();
	unique_ptr<AlterTableInfo> info;
	switch (type) {
	case AlterTableType::RENAME_COLUMN:
		return RenameColumnInfo::Deserialize(reader, schema, table);
	case AlterTableType::RENAME_TABLE:
		return RenameTableInfo::Deserialize(reader, schema, table);
	case AlterTableType::ADD_COLUMN:
		return AddColumnInfo::Deserialize(reader, schema, table);
	case AlterTableType::REMOVE_COLUMN:
		return RemoveColumnInfo::Deserialize(reader, schema, table);
	case AlterTableType::ALTER_COLUMN_TYPE:
		return ChangeColumnTypeInfo::Deserialize(reader, schema, table);
	case AlterTableType::SET_DEFAULT:
		return SetDefaultInfo::Deserialize(reader, schema, table);
	case AlterTableType::FOREIGN_KEY_CONSTRAINT:
		return AlterForeignKeyInfo::Deserialize(reader, schema, table);
	case AlterTableType::SET_NOT_NULL:
		return SetNotNullInfo::Deserialize(reader, schema, table);
	case AlterTableType::DROP_NOT_NULL:
		return DropNotNullInfo::Deserialize(reader, schema, table);
	default:
		throw SerializationException("Unknown alter table type for deserialization!");
	}
}

//===--------------------------------------------------------------------===//
// RenameColumnInfo
//===--------------------------------------------------------------------===//
RenameColumnInfo::RenameColumnInfo(string schema_p, string table_p, string old_name_p, string new_name_p)
    : AlterTableInfo(AlterTableType::RENAME_COLUMN, move(schema_p), move(table_p)), old_name(move(old_name_p)),
      new_name(move(new_name_p)) {
}
RenameColumnInfo::~RenameColumnInfo() {
}

unique_ptr<AlterInfo> RenameColumnInfo::Copy() const {
	return make_unique_base<AlterInfo, RenameColumnInfo>(schema, name, old_name, new_name);
}

void RenameColumnInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(old_name);
	writer.WriteString(new_name);
}

unique_ptr<AlterInfo> RenameColumnInfo::Deserialize(FieldReader &reader, string schema, string table) {
	auto old_name = reader.ReadRequired<string>();
	auto new_name = reader.ReadRequired<string>();
	return make_unique<RenameColumnInfo>(move(schema), move(table), old_name, new_name);
}

//===--------------------------------------------------------------------===//
// RenameTableInfo
//===--------------------------------------------------------------------===//
RenameTableInfo::RenameTableInfo(string schema_p, string table_p, string new_name_p)
    : AlterTableInfo(AlterTableType::RENAME_TABLE, move(schema_p), move(table_p)), new_table_name(move(new_name_p)) {
}
RenameTableInfo::~RenameTableInfo() {
}

unique_ptr<AlterInfo> RenameTableInfo::Copy() const {
	return make_unique_base<AlterInfo, RenameTableInfo>(schema, name, new_table_name);
}

void RenameTableInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(new_table_name);
}

unique_ptr<AlterInfo> RenameTableInfo::Deserialize(FieldReader &reader, string schema, string table) {
	auto new_name = reader.ReadRequired<string>();
	return make_unique<RenameTableInfo>(move(schema), move(table), new_name);
}

//===--------------------------------------------------------------------===//
// AddColumnInfo
//===--------------------------------------------------------------------===//
AddColumnInfo::AddColumnInfo(string schema_p, string table_p, ColumnDefinition new_column)
    : AlterTableInfo(AlterTableType::ADD_COLUMN, move(schema_p), move(table_p)), new_column(move(new_column)) {
}
AddColumnInfo::~AddColumnInfo() {
}

unique_ptr<AlterInfo> AddColumnInfo::Copy() const {
	return make_unique_base<AlterInfo, AddColumnInfo>(schema, name, new_column.Copy());
}

void AddColumnInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteSerializable(new_column);
}

unique_ptr<AlterInfo> AddColumnInfo::Deserialize(FieldReader &reader, string schema, string table) {
	auto new_column = reader.ReadRequiredSerializable<ColumnDefinition, ColumnDefinition>();
	return make_unique<AddColumnInfo>(move(schema), move(table), move(new_column));
}

//===--------------------------------------------------------------------===//
// RemoveColumnInfo
//===--------------------------------------------------------------------===//
RemoveColumnInfo::RemoveColumnInfo(string schema, string table, string removed_column, bool if_exists, bool cascade)
    : AlterTableInfo(AlterTableType::REMOVE_COLUMN, move(schema), move(table)), removed_column(move(removed_column)),
      if_exists(if_exists), cascade(cascade) {
}
RemoveColumnInfo::~RemoveColumnInfo() {
}

unique_ptr<AlterInfo> RemoveColumnInfo::Copy() const {
	return make_unique_base<AlterInfo, RemoveColumnInfo>(schema, name, removed_column, if_exists, cascade);
}

void RemoveColumnInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(removed_column);
	writer.WriteField<bool>(if_exists);
	writer.WriteField<bool>(cascade);
}

unique_ptr<AlterInfo> RemoveColumnInfo::Deserialize(FieldReader &reader, string schema, string table) {
	auto new_name = reader.ReadRequired<string>();
	auto if_exists = reader.ReadRequired<bool>();
	auto cascade = reader.ReadRequired<bool>();
	return make_unique<RemoveColumnInfo>(move(schema), move(table), new_name, if_exists, cascade);
}

//===--------------------------------------------------------------------===//
// ChangeColumnTypeInfo
//===--------------------------------------------------------------------===//
ChangeColumnTypeInfo::ChangeColumnTypeInfo(string schema_p, string table_p, string column_name, LogicalType target_type,
                                           unique_ptr<ParsedExpression> expression)
    : AlterTableInfo(AlterTableType::ALTER_COLUMN_TYPE, move(schema_p), move(table_p)), column_name(move(column_name)),
      target_type(move(target_type)), expression(move(expression)) {
}
ChangeColumnTypeInfo::~ChangeColumnTypeInfo() {
}

unique_ptr<AlterInfo> ChangeColumnTypeInfo::Copy() const {
	return make_unique_base<AlterInfo, ChangeColumnTypeInfo>(schema, name, column_name, target_type,
	                                                         expression->Copy());
}

void ChangeColumnTypeInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(column_name);
	writer.WriteSerializable(target_type);
	writer.WriteOptional(expression);
}

unique_ptr<AlterInfo> ChangeColumnTypeInfo::Deserialize(FieldReader &reader, string schema, string table) {
	auto column_name = reader.ReadRequired<string>();
	auto target_type = reader.ReadRequiredSerializable<LogicalType, LogicalType>();
	auto expression = reader.ReadOptional<ParsedExpression>(nullptr);
	return make_unique<ChangeColumnTypeInfo>(move(schema), move(table), move(column_name), move(target_type),
	                                         move(expression));
}

//===--------------------------------------------------------------------===//
// SetDefaultInfo
//===--------------------------------------------------------------------===//
SetDefaultInfo::SetDefaultInfo(string schema_p, string table_p, string column_name_p,
                               unique_ptr<ParsedExpression> new_default)
    : AlterTableInfo(AlterTableType::SET_DEFAULT, move(schema_p), move(table_p)), column_name(move(column_name_p)),
      expression(move(new_default)) {
}
SetDefaultInfo::~SetDefaultInfo() {
}

unique_ptr<AlterInfo> SetDefaultInfo::Copy() const {
	return make_unique_base<AlterInfo, SetDefaultInfo>(schema, name, column_name,
	                                                   expression ? expression->Copy() : nullptr);
}

void SetDefaultInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(column_name);
	writer.WriteOptional(expression);
}

unique_ptr<AlterInfo> SetDefaultInfo::Deserialize(FieldReader &reader, string schema, string table) {
	auto column_name = reader.ReadRequired<string>();
	auto new_default = reader.ReadOptional<ParsedExpression>(nullptr);
	return make_unique<SetDefaultInfo>(move(schema), move(table), move(column_name), move(new_default));
}

//===--------------------------------------------------------------------===//
// SetNotNullInfo
//===--------------------------------------------------------------------===//
SetNotNullInfo::SetNotNullInfo(string schema_p, string table_p, string column_name_p)
    : AlterTableInfo(AlterTableType::SET_NOT_NULL, move(schema_p), move(table_p)), column_name(move(column_name_p)) {
}
SetNotNullInfo::~SetNotNullInfo() {
}

unique_ptr<AlterInfo> SetNotNullInfo::Copy() const {
	return make_unique_base<AlterInfo, SetNotNullInfo>(schema, name, column_name);
}

void SetNotNullInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(column_name);
}

unique_ptr<AlterInfo> SetNotNullInfo::Deserialize(FieldReader &reader, string schema, string table) {
	auto column_name = reader.ReadRequired<string>();
	return make_unique<SetNotNullInfo>(move(schema), move(table), move(column_name));
}

//===--------------------------------------------------------------------===//
// DropNotNullInfo
//===--------------------------------------------------------------------===//
DropNotNullInfo::DropNotNullInfo(string schema_p, string table_p, string column_name_p)
    : AlterTableInfo(AlterTableType::DROP_NOT_NULL, move(schema_p), move(table_p)), column_name(move(column_name_p)) {
}
DropNotNullInfo::~DropNotNullInfo() {
}

unique_ptr<AlterInfo> DropNotNullInfo::Copy() const {
	return make_unique_base<AlterInfo, DropNotNullInfo>(schema, name, column_name);
}

void DropNotNullInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(column_name);
}

unique_ptr<AlterInfo> DropNotNullInfo::Deserialize(FieldReader &reader, string schema, string table) {
	auto column_name = reader.ReadRequired<string>();
	return make_unique<DropNotNullInfo>(move(schema), move(table), move(column_name));
}

//===--------------------------------------------------------------------===//
// AlterForeignKeyInfo
//===--------------------------------------------------------------------===//
AlterForeignKeyInfo::AlterForeignKeyInfo(string schema_p, string table_p, string fk_table, vector<string> pk_columns,
                                         vector<string> fk_columns, vector<idx_t> pk_keys, vector<idx_t> fk_keys,
                                         AlterForeignKeyType type_p)
    : AlterTableInfo(AlterTableType::FOREIGN_KEY_CONSTRAINT, move(schema_p), move(table_p)), fk_table(move(fk_table)),
      pk_columns(move(pk_columns)), fk_columns(move(fk_columns)), pk_keys(move(pk_keys)), fk_keys(move(fk_keys)),
      type(type_p) {
}
AlterForeignKeyInfo::~AlterForeignKeyInfo() {
}

unique_ptr<AlterInfo> AlterForeignKeyInfo::Copy() const {
	return make_unique_base<AlterInfo, AlterForeignKeyInfo>(schema, name, fk_table, pk_columns, fk_columns, pk_keys,
	                                                        fk_keys, type);
}

void AlterForeignKeyInfo::SerializeAlterTable(FieldWriter &writer) const {
	writer.WriteString(fk_table);
	writer.WriteList<string>(pk_columns);
	writer.WriteList<string>(fk_columns);
	writer.WriteList<idx_t>(pk_keys);
	writer.WriteList<idx_t>(fk_keys);
	writer.WriteField<AlterForeignKeyType>(type);
}

unique_ptr<AlterInfo> AlterForeignKeyInfo::Deserialize(FieldReader &reader, string schema, string table) {
	auto fk_table = reader.ReadRequired<string>();
	auto pk_columns = reader.ReadRequiredList<string>();
	auto fk_columns = reader.ReadRequiredList<string>();
	auto pk_keys = reader.ReadRequiredList<idx_t>();
	auto fk_keys = reader.ReadRequiredList<idx_t>();
	auto type = reader.ReadRequired<AlterForeignKeyType>();
	return make_unique<AlterForeignKeyInfo>(move(schema), move(table), move(fk_table), move(pk_columns),
	                                        move(fk_columns), move(pk_keys), move(fk_keys), type);
}

//===--------------------------------------------------------------------===//
// Alter View
//===--------------------------------------------------------------------===//
AlterViewInfo::AlterViewInfo(AlterViewType type, string schema_p, string view_p)
    : AlterInfo(AlterType::ALTER_VIEW, move(schema_p), move(view_p)), alter_view_type(type) {
}
AlterViewInfo::~AlterViewInfo() {
}

CatalogType AlterViewInfo::GetCatalogType() const {
	return CatalogType::VIEW_ENTRY;
}

void AlterViewInfo::Serialize(FieldWriter &writer) const {
	writer.WriteField<AlterViewType>(alter_view_type);
	writer.WriteString(schema);
	writer.WriteString(name);
	SerializeAlterView(writer);
}

unique_ptr<AlterInfo> AlterViewInfo::Deserialize(FieldReader &reader) {
	auto type = reader.ReadRequired<AlterViewType>();
	auto schema = reader.ReadRequired<string>();
	auto view = reader.ReadRequired<string>();
	unique_ptr<AlterViewInfo> info;
	switch (type) {
	case AlterViewType::RENAME_VIEW:
		return RenameViewInfo::Deserialize(reader, schema, view);
	default:
		throw SerializationException("Unknown alter view type for deserialization!");
	}
}

//===--------------------------------------------------------------------===//
// RenameViewInfo
//===--------------------------------------------------------------------===//
RenameViewInfo::RenameViewInfo(string schema_p, string view_p, string new_name_p)
    : AlterViewInfo(AlterViewType::RENAME_VIEW, move(schema_p), move(view_p)), new_view_name(move(new_name_p)) {
}
RenameViewInfo::~RenameViewInfo() {
}

unique_ptr<AlterInfo> RenameViewInfo::Copy() const {
	return make_unique_base<AlterInfo, RenameViewInfo>(schema, name, new_view_name);
}

void RenameViewInfo::SerializeAlterView(FieldWriter &writer) const {
	writer.WriteString(new_view_name);
}

unique_ptr<AlterInfo> RenameViewInfo::Deserialize(FieldReader &reader, string schema, string view) {
	auto new_name = reader.ReadRequired<string>();
	return make_unique<RenameViewInfo>(move(schema), move(view), new_name);
}
} // namespace duckdb
