//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/parser/column_definition.hpp"

namespace duckdb {

enum class AlterType : uint8_t { INVALID = 0, ALTER_TABLE = 1 };

struct AlterInfo : public ParseInfo {
	AlterInfo(AlterType type) : type(type) {
	}
	virtual ~AlterInfo() {
	}

	AlterType type;

	virtual void Serialize(Serializer &serializer);
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source);
};

enum class AlterTableType : uint8_t {
	INVALID = 0,
	RENAME_COLUMN = 1,
	RENAME_TABLE = 2,
	ADD_COLUMN = 3,
	REMOVE_COLUMN = 4,
	ALTER_COLUMN_TYPE = 5,
	SET_DEFAULT = 6
};

struct AlterTableInfo : public AlterInfo {
	AlterTableInfo(AlterTableType type, string schema, string table)
	    : AlterInfo(AlterType::ALTER_TABLE), alter_table_type(type), schema(schema), table(table) {
	}
	virtual ~AlterTableInfo() override {
	}

	AlterTableType alter_table_type;
	//! Schema name to alter to
	string schema;
	//! Table name to alter to
	string table;

public:
	virtual void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source);
};

//===--------------------------------------------------------------------===//
// RenameColumnInfo
//===--------------------------------------------------------------------===//
struct RenameColumnInfo : public AlterTableInfo {
	RenameColumnInfo(string schema, string table, string name, string new_name)
	    : AlterTableInfo(AlterTableType::RENAME_COLUMN, schema, table), name(name), new_name(new_name) {
	}
	~RenameColumnInfo() override {
	}

	//! Column old name
	string name;
	//! Column new name
	string new_name;

public:
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
};

//===--------------------------------------------------------------------===//
// RenameTableInfo
//===--------------------------------------------------------------------===//
struct RenameTableInfo : public AlterTableInfo {
	RenameTableInfo(string schema, string table, string new_name)
	    : AlterTableInfo(AlterTableType::RENAME_TABLE, schema, table), new_table_name(new_name) {
	}
	~RenameTableInfo() override {
	}

	//! Table new name
	string new_table_name;

public:
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
};

//===--------------------------------------------------------------------===//
// AddColumnInfo
//===--------------------------------------------------------------------===//
struct AddColumnInfo : public AlterTableInfo {
	AddColumnInfo(string schema, string table, ColumnDefinition new_column)
	    : AlterTableInfo(AlterTableType::ADD_COLUMN, schema, table), new_column(move(new_column)) {
	}
	~AddColumnInfo() override {
	}

	//! New column
	ColumnDefinition new_column;

public:
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
};

//===--------------------------------------------------------------------===//
// RemoveColumnInfo
//===--------------------------------------------------------------------===//
struct RemoveColumnInfo : public AlterTableInfo {
	RemoveColumnInfo(string schema, string table, string removed_column, bool if_exists)
	    : AlterTableInfo(AlterTableType::REMOVE_COLUMN, schema, table), removed_column(move(removed_column)),
	      if_exists(if_exists) {
	}
	~RemoveColumnInfo() override {
	}

	//! The column to remove
	string removed_column;
	//! Whether or not an error should be thrown if the column does not exist
	bool if_exists;

public:
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
};

//===--------------------------------------------------------------------===//
// ChangeColumnTypeInfo
//===--------------------------------------------------------------------===//
struct ChangeColumnTypeInfo : public AlterTableInfo {
	ChangeColumnTypeInfo(string schema, string table, string column_name, SQLType target_type,
	                     unique_ptr<ParsedExpression> expression)
	    : AlterTableInfo(AlterTableType::ALTER_COLUMN_TYPE, schema, table), column_name(move(column_name)),
	      target_type(move(target_type)), expression(move(expression)) {
	}
	~ChangeColumnTypeInfo() override {
	}

	//! The column name to alter
	string column_name;
	//! The target type of the column
	SQLType target_type;
	//! The expression used for data conversion
	unique_ptr<ParsedExpression> expression;

public:
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
};

//===--------------------------------------------------------------------===//
// SetDefaultInfo
//===--------------------------------------------------------------------===//
struct SetDefaultInfo : public AlterTableInfo {
	SetDefaultInfo(string schema, string table, string column_name, unique_ptr<ParsedExpression> new_default)
	    : AlterTableInfo(AlterTableType::SET_DEFAULT, schema, table), column_name(move(column_name)),
	      expression(move(new_default)) {
	}
	~SetDefaultInfo() override {
	}

	//! The column name to alter
	string column_name;
	//! The expression used for data conversion
	unique_ptr<ParsedExpression> expression;

public:
	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
};

} // namespace duckdb
