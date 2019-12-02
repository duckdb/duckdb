//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/alter_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

enum class AlterType : uint8_t { INVALID = 0, ALTER_TABLE = 1 };

struct AlterInfo {
	AlterType type;

	AlterInfo(AlterType type) : type(type) {
	}
};

enum class AlterTableType : uint8_t { INVALID = 0, RENAME_COLUMN = 1 };

struct AlterTableInfo : public AlterInfo {
	AlterTableType alter_table_type;
	//! Schema name to alter to
	string schema;
	//! Table name to alter to
	string table;

	AlterTableInfo(AlterTableType type, string schema, string table)
	    : AlterInfo(AlterType::ALTER_TABLE), alter_table_type(type), schema(schema), table(table) {
	}
};

struct RenameColumnInfo : public AlterTableInfo {
	//! Column old name
	string name;
	//! Column new name
	string new_name;

	RenameColumnInfo(string schema, string table, string name, string new_name)
	    : AlterTableInfo(AlterTableType::RENAME_COLUMN, schema, table), name(name), new_name(new_name) {
	}
};

} // namespace duckdb
