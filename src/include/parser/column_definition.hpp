//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/column_definition.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/value.hpp"

namespace duckdb {

//! A column of a table.
class ColumnDefinition {
public:
	ColumnDefinition(string name, SQLType type) : name(name), type(type), has_default(false) {
	}
	ColumnDefinition(string name, SQLType type, Value default_value)
	    : name(name), type(type), has_default(true), default_value(default_value) {
	}

	//! The name of the entry
	string name;
	//! The index of the column in the table
	size_t oid;
	//! The type of the column
	SQLType type;
	//! Whether or not the column has a default value
	bool has_default;
	//! The default value of the column (if any)
	Value default_value;
};
} // namespace duckdb
