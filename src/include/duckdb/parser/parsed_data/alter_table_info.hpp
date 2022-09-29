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
#include "duckdb/parser/parsed_data/parse_info.hpp"

namespace duckdb {

enum AlterForeignKeyType : uint8_t { AFT_ADD = 0, AFT_DELETE = 1 };

//===--------------------------------------------------------------------===//
// Change Ownership
//===--------------------------------------------------------------------===//
struct ChangeOwnershipInfo : public AlterInfo {
	ChangeOwnershipInfo(CatalogType entry_catalog_type, string entry_schema, string entry_name, string owner_schema,
	                    string owner_name, bool if_exists);

	// Catalog type refers to the entry type, since this struct is usually built from an
	// ALTER <TYPE> <schema>.<name> OWNED BY <owner_schema>.<owner_name> statement
	// here it is only possible to know the type of who is to be owned
	CatalogType entry_catalog_type;

	string owner_schema;
	string owner_name;

public:
	CatalogType GetCatalogType() const override;
	unique_ptr<AlterInfo> Copy() const override;
	void Serialize(FieldWriter &writer) const override;
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
	DROP_NOT_NULL = 9
};

struct AlterTableInfo : public AlterInfo {
	AlterTableInfo(AlterTableType type, string schema, string table, bool if_exists);
	~AlterTableInfo() override;

	AlterTableType alter_table_type;

public:
	CatalogType GetCatalogType() const override;
	void Serialize(FieldWriter &writer) const override;
	virtual void SerializeAlterTable(FieldWriter &writer) const = 0;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader);
};

//===--------------------------------------------------------------------===//
// RenameColumnInfo
//===--------------------------------------------------------------------===//
struct RenameColumnInfo : public AlterTableInfo {
	RenameColumnInfo(string schema, string table, bool if_exists, string old_name_p, string new_name_p);
	~RenameColumnInfo() override;

	//! Column old name
	string old_name;
	//! Column new name
	string new_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void SerializeAlterTable(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string table, bool if_exists);
};

//===--------------------------------------------------------------------===//
// RenameTableInfo
//===--------------------------------------------------------------------===//
struct RenameTableInfo : public AlterTableInfo {
	RenameTableInfo(string schema, string table, bool if_exists, string new_name);
	~RenameTableInfo() override;

	//! Relation new name
	string new_table_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void SerializeAlterTable(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string table, bool if_exists);
};

//===--------------------------------------------------------------------===//
// AddColumnInfo
//===--------------------------------------------------------------------===//
struct AddColumnInfo : public AlterTableInfo {
	AddColumnInfo(string schema, string table, bool if_exists, ColumnDefinition new_column, bool if_column_not_exists);
	~AddColumnInfo() override;

	//! New column
	ColumnDefinition new_column;
	//! Whether or not an error should be thrown if the column exist
	bool if_column_not_exists;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void SerializeAlterTable(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string table, bool if_exists);
};

//===--------------------------------------------------------------------===//
// RemoveColumnInfo
//===--------------------------------------------------------------------===//
struct RemoveColumnInfo : public AlterTableInfo {
	RemoveColumnInfo(string schema, string table, bool if_exists, string removed_column, bool if_column_exists,
	                 bool cascade);
	~RemoveColumnInfo() override;

	//! The column to remove
	string removed_column;
	//! Whether or not an error should be thrown if the column does not exist
	bool if_column_exists;
	//! Whether or not the column should be removed if a dependency conflict arises (used by GENERATED columns)
	bool cascade;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void SerializeAlterTable(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string table, bool if_exists);
};

//===--------------------------------------------------------------------===//
// ChangeColumnTypeInfo
//===--------------------------------------------------------------------===//
struct ChangeColumnTypeInfo : public AlterTableInfo {
	ChangeColumnTypeInfo(string schema, string table, bool if_exists, string column_name, LogicalType target_type,
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
	void SerializeAlterTable(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string table, bool if_exists);
};

//===--------------------------------------------------------------------===//
// SetDefaultInfo
//===--------------------------------------------------------------------===//
struct SetDefaultInfo : public AlterTableInfo {
	SetDefaultInfo(string schema, string table, bool if_exists, string column_name,
	               unique_ptr<ParsedExpression> new_default);
	~SetDefaultInfo() override;

	//! The column name to alter
	string column_name;
	//! The expression used for data conversion
	unique_ptr<ParsedExpression> expression;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void SerializeAlterTable(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string table, bool if_exists);
};

//===--------------------------------------------------------------------===//
// AlterForeignKeyInfo
//===--------------------------------------------------------------------===//
struct AlterForeignKeyInfo : public AlterTableInfo {
	AlterForeignKeyInfo(string schema, string table, bool if_exists, string fk_table, vector<string> pk_columns,
	                    vector<string> fk_columns, vector<idx_t> pk_keys, vector<idx_t> fk_keys,
	                    AlterForeignKeyType type);
	~AlterForeignKeyInfo() override;

	string fk_table;
	vector<string> pk_columns;
	vector<string> fk_columns;
	vector<idx_t> pk_keys;
	vector<idx_t> fk_keys;
	AlterForeignKeyType type;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void SerializeAlterTable(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string table, bool if_exists);
};

//===--------------------------------------------------------------------===//
// Alter View
//===--------------------------------------------------------------------===//
enum class AlterViewType : uint8_t { INVALID = 0, RENAME_VIEW = 1 };

struct AlterViewInfo : public AlterInfo {
	AlterViewInfo(AlterViewType type, string schema, string view, bool if_exists);
	~AlterViewInfo() override;

	AlterViewType alter_view_type;

public:
	CatalogType GetCatalogType() const override;
	void Serialize(FieldWriter &writer) const override;
	virtual void SerializeAlterView(FieldWriter &writer) const = 0;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader);
};

//===--------------------------------------------------------------------===//
// RenameViewInfo
//===--------------------------------------------------------------------===//
struct RenameViewInfo : public AlterViewInfo {
	RenameViewInfo(string schema, string view, bool if_exists, string new_name);
	~RenameViewInfo() override;

	//! Relation new name
	string new_view_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void SerializeAlterView(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string view, bool if_exists);
};

//===--------------------------------------------------------------------===//
// SetNotNullInfo
//===--------------------------------------------------------------------===//
struct SetNotNullInfo : public AlterTableInfo {
	SetNotNullInfo(string schema, string table, bool if_exists, string column_name);
	~SetNotNullInfo() override;

	//! The column name to alter
	string column_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void SerializeAlterTable(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string table, bool if_exists);
};

//===--------------------------------------------------------------------===//
// DropNotNullInfo
//===--------------------------------------------------------------------===//
struct DropNotNullInfo : public AlterTableInfo {
	DropNotNullInfo(string schema, string table, bool if_exists, string column_name);
	~DropNotNullInfo() override;

	//! The column name to alter
	string column_name;

public:
	unique_ptr<AlterInfo> Copy() const override;
	void SerializeAlterTable(FieldWriter &writer) const override;
	static unique_ptr<AlterInfo> Deserialize(FieldReader &reader, string schema, string table, bool if_exists);
};

} // namespace duckdb
