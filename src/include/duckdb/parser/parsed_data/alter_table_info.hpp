//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/alter_info.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"
#include "duckdb/parser/result_modifier.hpp"

namespace duckdb {

enum class AlterForeignKeyType : uint8_t { AFT_ADD = 0, AFT_DELETE = 1 };

//===--------------------------------------------------------------------===//
// Change Ownership
//===--------------------------------------------------------------------===//
struct ChangeOwnershipInfo : public AlterInfo {
	ChangeOwnershipInfo(CatalogType entry_catalog_type, string entry_catalog, string entry_schema, string entry_name,
	                    string owner_schema, string owner_name, OnEntryNotFound if_not_found);

	// Catalog type refers to the entry type, since this struct is usually built from an
	// ALTER <TYPE> <schema>.<name> OWNED BY <owner_schema>.<owner_name> statement
	// here it is only possible to know the type of who is to be owned
	CatalogType entry_catalog_type;

	string owner_schema;
	string owner_name;

public:
	CatalogType GetCatalogType() const override;
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &deserializer);

	explicit ChangeOwnershipInfo();
};

//===--------------------------------------------------------------------===//
// Set Comment
//===--------------------------------------------------------------------===//
struct SetCommentInfo : public AlterInfo {
	SetCommentInfo(CatalogType entry_catalog_type, string entry_catalog, string entry_schema, string entry_name,
	               Value new_comment_value_p, OnEntryNotFound if_not_found);

	CatalogType entry_catalog_type;
	Value comment_value;

public:
	CatalogType GetCatalogType() const override;
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &deserializer);

	explicit SetCommentInfo();
};

//===--------------------------------------------------------------------===//
// Alter Table
//===--------------------------------------------------------------------===//
enum class AlterTableType : uint8_t {
	INVALID = 0,
	RENAME_COLUMN = 1,
	RENAME_TABLE = 2,
	ADD_COLUMN = 3,
	REMOVE_COLUMN = 4,
	ALTER_COLUMN_TYPE = 5,
	SET_DEFAULT = 6,
	FOREIGN_KEY_CONSTRAINT = 7,
	SET_NOT_NULL = 8,
	DROP_NOT_NULL = 9,
	SET_COLUMN_COMMENT = 10,
	ADD_CONSTRAINT = 11,
	SET_PARTITIONED_BY = 12,
	SET_SORTED_BY = 13,
	ADD_FIELD = 14,
	REMOVE_FIELD = 15,
	RENAME_FIELD = 16
};

struct AlterTableInfo : public AlterInfo {
	AlterTableInfo(AlterTableType type, AlterEntryData data);
	~AlterTableInfo() override;

	AlterTableType alter_table_type;

public:
	CatalogType GetCatalogType() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &deserializer);

protected:
	explicit AlterTableInfo(AlterTableType type);
};

//===--------------------------------------------------------------------===//
// RenameColumnInfo
//===--------------------------------------------------------------------===//
struct RenameColumnInfo : public AlterTableInfo {
	RenameColumnInfo(AlterEntryData data, string old_name_p, string new_name_p);
	~RenameColumnInfo() override;

	//! Column old name
	string old_name;
	//! Column new name
	string new_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	RenameColumnInfo();
};

//===--------------------------------------------------------------------===//
// RenameFieldInfo
//===--------------------------------------------------------------------===//
struct RenameFieldInfo : public AlterTableInfo {
	RenameFieldInfo(AlterEntryData data, vector<string> column_path, string new_name_p);
	~RenameFieldInfo() override;

	//! Path to source field.
	vector<string> column_path;
	//! New name of the column (field).
	string new_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	string GetColumnName() const override {
		return column_path[0];
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	RenameFieldInfo();
};

//===--------------------------------------------------------------------===//
// RenameTableInfo
//===--------------------------------------------------------------------===//
struct RenameTableInfo : public AlterTableInfo {
	RenameTableInfo(AlterEntryData data, string new_name);
	~RenameTableInfo() override;

	//! Relation new name
	string new_table_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	RenameTableInfo();
};

//===--------------------------------------------------------------------===//
// AddColumnInfo
//===--------------------------------------------------------------------===//
struct AddColumnInfo : public AlterTableInfo {
	AddColumnInfo(AlterEntryData data, ColumnDefinition new_column, bool if_column_not_exists);
	~AddColumnInfo() override;

	//! New column
	ColumnDefinition new_column;
	//! Whether or not an error should be thrown if the column exist
	bool if_column_not_exists;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	explicit AddColumnInfo(ColumnDefinition new_column);
};

//===--------------------------------------------------------------------===//
// AddFieldInfo
//===--------------------------------------------------------------------===//
struct AddFieldInfo : public AlterTableInfo {
	AddFieldInfo(AlterEntryData data, vector<string> column_path, ColumnDefinition new_field, bool if_field_not_exists);
	~AddFieldInfo() override;

	//! Path to source field.
	vector<string> column_path;
	//! New field to add.
	ColumnDefinition new_field;
	//! Whether or not an error should be thrown if the field does not exist.
	bool if_field_not_exists;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	string GetColumnName() const override {
		return column_path[0];
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	explicit AddFieldInfo(ColumnDefinition new_column);
};

//===--------------------------------------------------------------------===//
// RemoveColumnInfo
//===--------------------------------------------------------------------===//
struct RemoveColumnInfo : public AlterTableInfo {
	RemoveColumnInfo(AlterEntryData data, string removed_column, bool if_column_exists, bool cascade);
	~RemoveColumnInfo() override;

	//! The column to remove
	string removed_column;
	//! Whether or not an error should be thrown if the column does not exist
	bool if_column_exists;
	//! Whether or not the column should be removed if a dependency conflict arises (used by GENERATED columns)
	bool cascade;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);
	string GetColumnName() const override {
		return removed_column;
	}

private:
	RemoveColumnInfo();
};

//===--------------------------------------------------------------------===//
// RemoveFieldInfo
//===--------------------------------------------------------------------===//
struct RemoveFieldInfo : public AlterTableInfo {
	RemoveFieldInfo(AlterEntryData data, vector<string> column_path, bool if_column_exists, bool cascade);
	~RemoveFieldInfo() override;

	//! Path to source field.
	vector<string> column_path;
	//! Whether or not an error should be thrown if the column does not exist.
	bool if_column_exists;
	//! Whether or not the column should be removed if a dependency conflict arises (used by GENERATED columns).
	bool cascade;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	string GetColumnName() const override {
		return column_path[0];
	}

	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	RemoveFieldInfo();
};
//===--------------------------------------------------------------------===//
// ChangeColumnTypeInfo
//===--------------------------------------------------------------------===//
struct ChangeColumnTypeInfo : public AlterTableInfo {
	ChangeColumnTypeInfo(AlterEntryData data, string column_name, LogicalType target_type,
	                     unique_ptr<ParsedExpression> expression);
	~ChangeColumnTypeInfo() override;

	//! The column name to alter
	string column_name;
	//! The target type of the column
	LogicalType target_type;
	//! The expression used for data conversion
	unique_ptr<ParsedExpression> expression;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);
	string GetColumnName() const override {
		return column_name;
	};

private:
	ChangeColumnTypeInfo();
};

//===--------------------------------------------------------------------===//
// SetDefaultInfo
//===--------------------------------------------------------------------===//
struct SetDefaultInfo : public AlterTableInfo {
	SetDefaultInfo(AlterEntryData data, string column_name, unique_ptr<ParsedExpression> new_default);
	~SetDefaultInfo() override;

	//! The column name to alter
	string column_name;
	//! The expression used for data conversion
	unique_ptr<ParsedExpression> expression;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	SetDefaultInfo();
};

//===--------------------------------------------------------------------===//
// AlterForeignKeyInfo
//===--------------------------------------------------------------------===//
struct AlterForeignKeyInfo : public AlterTableInfo {
	AlterForeignKeyInfo(AlterEntryData data, string fk_table, vector<string> pk_columns, vector<string> fk_columns,
	                    vector<PhysicalIndex> pk_keys, vector<PhysicalIndex> fk_keys, AlterForeignKeyType type);
	~AlterForeignKeyInfo() override;

	string fk_table;
	vector<string> pk_columns;
	vector<string> fk_columns;
	vector<PhysicalIndex> pk_keys;
	vector<PhysicalIndex> fk_keys;
	AlterForeignKeyType type;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	AlterForeignKeyInfo();
};

//===--------------------------------------------------------------------===//
// SetNotNullInfo
//===--------------------------------------------------------------------===//
struct SetNotNullInfo : public AlterTableInfo {
	SetNotNullInfo(AlterEntryData data, string column_name);
	~SetNotNullInfo() override;

	//! The column name to alter
	string column_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	SetNotNullInfo();
};

//===--------------------------------------------------------------------===//
// DropNotNullInfo
//===--------------------------------------------------------------------===//
struct DropNotNullInfo : public AlterTableInfo {
	DropNotNullInfo(AlterEntryData data, string column_name);
	~DropNotNullInfo() override;

	//! The column name to alter
	string column_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	DropNotNullInfo();
};

//===--------------------------------------------------------------------===//
// Alter View
//===--------------------------------------------------------------------===//
enum class AlterViewType : uint8_t { INVALID = 0, RENAME_VIEW = 1 };

struct AlterViewInfo : public AlterInfo {
	AlterViewInfo(AlterViewType type, AlterEntryData data);
	~AlterViewInfo() override;

	AlterViewType alter_view_type;

public:
	CatalogType GetCatalogType() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &deserializer);

protected:
	explicit AlterViewInfo(AlterViewType type);
};

//===--------------------------------------------------------------------===//
// RenameViewInfo
//===--------------------------------------------------------------------===//
struct RenameViewInfo : public AlterViewInfo {
	RenameViewInfo(AlterEntryData data, string new_name);
	~RenameViewInfo() override;

	//! Relation new name
	string new_view_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterViewInfo> Deserialize(Deserializer &deserializer);

private:
	RenameViewInfo();
};

//===--------------------------------------------------------------------===//
// AddConstraintInfo
//===--------------------------------------------------------------------===//
struct AddConstraintInfo : public AlterTableInfo {
	AddConstraintInfo(AlterEntryData data, unique_ptr<Constraint> constraint);
	~AddConstraintInfo() override;

	//! The constraint to add.
	unique_ptr<Constraint> constraint;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	AddConstraintInfo();
};

//===--------------------------------------------------------------------===//
// SetPartitionedByInfo
//===--------------------------------------------------------------------===//
struct SetPartitionedByInfo : public AlterTableInfo {
	SetPartitionedByInfo(AlterEntryData data, vector<unique_ptr<ParsedExpression>> partition_keys);
	~SetPartitionedByInfo() override;

	//! The partition keys
	vector<unique_ptr<ParsedExpression>> partition_keys;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	SetPartitionedByInfo();
};

//===--------------------------------------------------------------------===//
// SetSortedByInfo
//===--------------------------------------------------------------------===//
struct SetSortedByInfo : public AlterTableInfo {
	SetSortedByInfo(AlterEntryData data, vector<OrderByNode> orders);
	~SetSortedByInfo() override;

	//! The sort keys
	vector<OrderByNode> orders;

public:
	unique_ptr<AlterInfo> Copy() const override;
	string ToString() const override;
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<AlterTableInfo> Deserialize(Deserializer &deserializer);

private:
	SetSortedByInfo();
};

} // namespace duckdb
