//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/alter_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"

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

enum class AlterTableType : uint8_t { INVALID = 0, RENAME_COLUMN = 1, RENAME_TABLE = 2};

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

	virtual void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source);
};

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

	void Serialize(Serializer &serializer) override;
	static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
};

struct RenameTableInfo : public AlterTableInfo {
    RenameTableInfo(string schema, string table, string new_name)
            : AlterTableInfo(AlterTableType::RENAME_TABLE, schema, table), new_table_name(new_name) {
    }
    ~RenameTableInfo() override {
    }

    //! Table new name
    string new_table_name;

    void Serialize(Serializer &serializer) override;
    static unique_ptr<AlterInfo> Deserialize(Deserializer &source, string schema, string table);
};

} // namespace duckdb
