//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/parser/column_definition.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {
struct CreateTableInfo : public ParseInfo {
	CreateTableInfo() : schema(INVALID_SCHEMA), if_not_exists(false), temporary(false) {
	}
	CreateTableInfo(string schema, string name) : schema(schema), table(name), if_not_exists(false), temporary(false) {
	}

	//! Schema name to insert to
	string schema;
	//! Table name to insert to
	string table;
	//! List of columns of the table
	vector<ColumnDefinition> columns;
	//! List of constraints on the table
	vector<unique_ptr<Constraint>> constraints;
	//! Ignore if the entry already exists, instead of failing
	bool if_not_exists = false;
	//! Whether or not it is a temporary table
	bool temporary = false;
};

} // namespace duckdb
