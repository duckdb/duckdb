//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/parsed_data.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/internal_types.hpp"

#include "parser/column_definition.hpp"

namespace duckdb {
class Constraint;

struct CreateTableInformation {
	//! Schema name to insert to
	std::string schema;
	//! Table name to insert to
	std::string table;
	//! List of columns of the table
	std::vector<ColumnDefinition> columns;
	//! List of constraints on the table
	std::vector<std::unique_ptr<Constraint>> constraints;
	//! Ignore if the entry already exists, instead of failing
	bool if_not_exists = false;

	CreateTableInformation() : schema(DEFAULT_SCHEMA), if_not_exists(false) {}
	CreateTableInformation(std::string schema, std::string table,
	                       std::vector<ColumnDefinition> columns)
	    : schema(schema), table(table), columns(columns), if_not_exists(false) {
	}
};

struct DropTableInformation {
	//! Schema name to drop from
	std::string schema;
	//! Table name to drop
	std::string table;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;

	DropTableInformation()
	    : schema(DEFAULT_SCHEMA), if_exists(false), cascade(false) {}
};

struct CreateSchemaInformation {
	//! Schema name to create
	std::string schema;
	//! Ignore if the entry already exists, instead of failing
	bool if_not_exists = false;

	CreateSchemaInformation() : if_not_exists(false) {}
};

struct DropSchemaInformation {
	//! Schema name to drop
	std::string schema;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;

	DropSchemaInformation() : if_exists(false), cascade(false) {}
};

} // namespace duckdb
