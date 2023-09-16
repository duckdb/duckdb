#include "duckdb/parser/parsed_data/alter_table_info.hpp"

#include "duckdb/parser/constraint.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// ChangeOwnershipInfo
//===--------------------------------------------------------------------===//
ChangeOwnershipInfo::ChangeOwnershipInfo(CatalogType entry_catalog_type, string entry_catalog_p, string entry_schema_p,
                                         string entry_name_p, string owner_schema_p, string owner_name_p,
                                         OnEntryNotFound if_not_found)
    : AlterInfo(AlterType::CHANGE_OWNERSHIP, std::move(entry_catalog_p), std::move(entry_schema_p),
                std::move(entry_name_p), if_not_found),
      entry_catalog_type(entry_catalog_type), owner_schema(std::move(owner_schema_p)),
      owner_name(std::move(owner_name_p)) {
}

CatalogType ChangeOwnershipInfo::GetCatalogType() const {
	return entry_catalog_type;
}

unique_ptr<AlterInfo> ChangeOwnershipInfo::Copy() const {
	return make_uniq_base<AlterInfo, ChangeOwnershipInfo>(entry_catalog_type, catalog, schema, name, owner_schema,
	                                                      owner_name, if_not_found);
}

//===--------------------------------------------------------------------===//
// AlterTableInfo
//===--------------------------------------------------------------------===//
AlterTableInfo::AlterTableInfo(AlterTableType type) : AlterInfo(AlterType::ALTER_TABLE), alter_table_type(type) {
}

AlterTableInfo::AlterTableInfo(AlterTableType type, AlterEntryData data)
    : AlterInfo(AlterType::ALTER_TABLE, std::move(data.catalog), std::move(data.schema), std::move(data.name),
                data.if_not_found),
      alter_table_type(type) {
}
AlterTableInfo::~AlterTableInfo() {
}

CatalogType AlterTableInfo::GetCatalogType() const {
	return CatalogType::TABLE_ENTRY;
}
//===--------------------------------------------------------------------===//
// RenameColumnInfo
//===--------------------------------------------------------------------===//
RenameColumnInfo::RenameColumnInfo(AlterEntryData data, string old_name_p, string new_name_p)
    : AlterTableInfo(AlterTableType::RENAME_COLUMN, std::move(data)), old_name(std::move(old_name_p)),
      new_name(std::move(new_name_p)) {
}

RenameColumnInfo::RenameColumnInfo() : AlterTableInfo(AlterTableType::RENAME_COLUMN) {
}

RenameColumnInfo::~RenameColumnInfo() {
}

unique_ptr<AlterInfo> RenameColumnInfo::Copy() const {
	return make_uniq_base<AlterInfo, RenameColumnInfo>(GetAlterEntryData(), old_name, new_name);
}

//===--------------------------------------------------------------------===//
// RenameTableInfo
//===--------------------------------------------------------------------===//
RenameTableInfo::RenameTableInfo() : AlterTableInfo(AlterTableType::RENAME_TABLE) {
}

RenameTableInfo::RenameTableInfo(AlterEntryData data, string new_name_p)
    : AlterTableInfo(AlterTableType::RENAME_TABLE, std::move(data)), new_table_name(std::move(new_name_p)) {
}

RenameTableInfo::~RenameTableInfo() {
}

unique_ptr<AlterInfo> RenameTableInfo::Copy() const {
	return make_uniq_base<AlterInfo, RenameTableInfo>(GetAlterEntryData(), new_table_name);
}

//===--------------------------------------------------------------------===//
// AddColumnInfo
//===--------------------------------------------------------------------===//
AddColumnInfo::AddColumnInfo(ColumnDefinition new_column_p)
    : AlterTableInfo(AlterTableType::ADD_COLUMN), new_column(std::move(new_column_p)) {
}

AddColumnInfo::AddColumnInfo(AlterEntryData data, ColumnDefinition new_column, bool if_column_not_exists)
    : AlterTableInfo(AlterTableType::ADD_COLUMN, std::move(data)), new_column(std::move(new_column)),
      if_column_not_exists(if_column_not_exists) {
}

AddColumnInfo::~AddColumnInfo() {
}

unique_ptr<AlterInfo> AddColumnInfo::Copy() const {
	return make_uniq_base<AlterInfo, AddColumnInfo>(GetAlterEntryData(), new_column.Copy(), if_column_not_exists);
}

//===--------------------------------------------------------------------===//
// RemoveColumnInfo
//===--------------------------------------------------------------------===//
RemoveColumnInfo::RemoveColumnInfo() : AlterTableInfo(AlterTableType::REMOVE_COLUMN) {
}

RemoveColumnInfo::RemoveColumnInfo(AlterEntryData data, string removed_column, bool if_column_exists, bool cascade)
    : AlterTableInfo(AlterTableType::REMOVE_COLUMN, std::move(data)), removed_column(std::move(removed_column)),
      if_column_exists(if_column_exists), cascade(cascade) {
}
RemoveColumnInfo::~RemoveColumnInfo() {
}

unique_ptr<AlterInfo> RemoveColumnInfo::Copy() const {
	return make_uniq_base<AlterInfo, RemoveColumnInfo>(GetAlterEntryData(), removed_column, if_column_exists, cascade);
}

//===--------------------------------------------------------------------===//
// ChangeColumnTypeInfo
//===--------------------------------------------------------------------===//
ChangeColumnTypeInfo::ChangeColumnTypeInfo() : AlterTableInfo(AlterTableType::ALTER_COLUMN_TYPE) {
}

ChangeColumnTypeInfo::ChangeColumnTypeInfo(AlterEntryData data, string column_name, LogicalType target_type,
                                           unique_ptr<ParsedExpression> expression)
    : AlterTableInfo(AlterTableType::ALTER_COLUMN_TYPE, std::move(data)), column_name(std::move(column_name)),
      target_type(std::move(target_type)), expression(std::move(expression)) {
}
ChangeColumnTypeInfo::~ChangeColumnTypeInfo() {
}

unique_ptr<AlterInfo> ChangeColumnTypeInfo::Copy() const {
	return make_uniq_base<AlterInfo, ChangeColumnTypeInfo>(GetAlterEntryData(), column_name, target_type,
	                                                       expression->Copy());
}

//===--------------------------------------------------------------------===//
// SetDefaultInfo
//===--------------------------------------------------------------------===//
SetDefaultInfo::SetDefaultInfo() : AlterTableInfo(AlterTableType::SET_DEFAULT) {
}

SetDefaultInfo::SetDefaultInfo(AlterEntryData data, string column_name_p, unique_ptr<ParsedExpression> new_default)
    : AlterTableInfo(AlterTableType::SET_DEFAULT, std::move(data)), column_name(std::move(column_name_p)),
      expression(std::move(new_default)) {
}
SetDefaultInfo::~SetDefaultInfo() {
}

unique_ptr<AlterInfo> SetDefaultInfo::Copy() const {
	return make_uniq_base<AlterInfo, SetDefaultInfo>(GetAlterEntryData(), column_name,
	                                                 expression ? expression->Copy() : nullptr);
}

//===--------------------------------------------------------------------===//
// SetNotNullInfo
//===--------------------------------------------------------------------===//
SetNotNullInfo::SetNotNullInfo() : AlterTableInfo(AlterTableType::SET_NOT_NULL) {
}

SetNotNullInfo::SetNotNullInfo(AlterEntryData data, string column_name_p)
    : AlterTableInfo(AlterTableType::SET_NOT_NULL, std::move(data)), column_name(std::move(column_name_p)) {
}
SetNotNullInfo::~SetNotNullInfo() {
}

unique_ptr<AlterInfo> SetNotNullInfo::Copy() const {
	return make_uniq_base<AlterInfo, SetNotNullInfo>(GetAlterEntryData(), column_name);
}

//===--------------------------------------------------------------------===//
// DropNotNullInfo
//===--------------------------------------------------------------------===//
DropNotNullInfo::DropNotNullInfo() : AlterTableInfo(AlterTableType::DROP_NOT_NULL) {
}

DropNotNullInfo::DropNotNullInfo(AlterEntryData data, string column_name_p)
    : AlterTableInfo(AlterTableType::DROP_NOT_NULL, std::move(data)), column_name(std::move(column_name_p)) {
}
DropNotNullInfo::~DropNotNullInfo() {
}

unique_ptr<AlterInfo> DropNotNullInfo::Copy() const {
	return make_uniq_base<AlterInfo, DropNotNullInfo>(GetAlterEntryData(), column_name);
}

//===--------------------------------------------------------------------===//
// AlterForeignKeyInfo
//===--------------------------------------------------------------------===//
AlterForeignKeyInfo::AlterForeignKeyInfo() : AlterTableInfo(AlterTableType::FOREIGN_KEY_CONSTRAINT) {
}

AlterForeignKeyInfo::AlterForeignKeyInfo(AlterEntryData data, string fk_table, vector<string> pk_columns,
                                         vector<string> fk_columns, vector<PhysicalIndex> pk_keys,
                                         vector<PhysicalIndex> fk_keys, AlterForeignKeyType type_p)
    : AlterTableInfo(AlterTableType::FOREIGN_KEY_CONSTRAINT, std::move(data)), fk_table(std::move(fk_table)),
      pk_columns(std::move(pk_columns)), fk_columns(std::move(fk_columns)), pk_keys(std::move(pk_keys)),
      fk_keys(std::move(fk_keys)), type(type_p) {
}
AlterForeignKeyInfo::~AlterForeignKeyInfo() {
}

unique_ptr<AlterInfo> AlterForeignKeyInfo::Copy() const {
	return make_uniq_base<AlterInfo, AlterForeignKeyInfo>(GetAlterEntryData(), fk_table, pk_columns, fk_columns,
	                                                      pk_keys, fk_keys, type);
}

//===--------------------------------------------------------------------===//
// Alter View
//===--------------------------------------------------------------------===//
AlterViewInfo::AlterViewInfo(AlterViewType type) : AlterInfo(AlterType::ALTER_VIEW), alter_view_type(type) {
}

AlterViewInfo::AlterViewInfo(AlterViewType type, AlterEntryData data)
    : AlterInfo(AlterType::ALTER_VIEW, std::move(data.catalog), std::move(data.schema), std::move(data.name),
                data.if_not_found),
      alter_view_type(type) {
}
AlterViewInfo::~AlterViewInfo() {
}

CatalogType AlterViewInfo::GetCatalogType() const {
	return CatalogType::VIEW_ENTRY;
}

//===--------------------------------------------------------------------===//
// RenameViewInfo
//===--------------------------------------------------------------------===//
RenameViewInfo::RenameViewInfo() : AlterViewInfo(AlterViewType::RENAME_VIEW) {
}
RenameViewInfo::RenameViewInfo(AlterEntryData data, string new_name_p)
    : AlterViewInfo(AlterViewType::RENAME_VIEW, std::move(data)), new_view_name(std::move(new_name_p)) {
}
RenameViewInfo::~RenameViewInfo() {
}

unique_ptr<AlterInfo> RenameViewInfo::Copy() const {
	return make_uniq_base<AlterInfo, RenameViewInfo>(GetAlterEntryData(), new_view_name);
}

} // namespace duckdb
