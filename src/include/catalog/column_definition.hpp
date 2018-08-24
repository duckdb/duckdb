//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// catalog/column_definition.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"
#include "common/types/value.hpp"

namespace duckdb {

//! A column of a table.
class ColumnDefinition {
  public:
	ColumnDefinition(std::string name, TypeId type, bool is_not_null);
	ColumnDefinition(std::string name, TypeId type, bool is_not_null,
	                 Value default_value);

	//! The name of the entry
	std::string name;
	//! The index of the column in the table
	size_t oid;
	//! The type of the column
	TypeId type;
	//! Whether or not the column can contain NULL values
	bool is_not_null;
	//! Whether or not the column has a default value
	bool has_default;
	//! The default value of the column (if any)
	Value default_value;

	virtual std::string ToString() const { return std::string(); }
};
} // namespace duckdb
