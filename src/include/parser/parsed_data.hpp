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

#include "function/function.hpp"

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

struct CreateTableFunctionInformation {
	//! Schema name
	std::string schema;
	//! Function name
	std::string name;
	//! Replace function if it already exists instead of failing
	bool or_replace = false;
	//! List of return columns
	std::vector<ColumnDefinition> return_values;
	//! Input arguments
	std::vector<TypeId> arguments;
	//! Initialize function pointer
	table_function_init_t init;
	//! The function pointer
	table_function_t function;
	//! Final function pointer
	table_function_final_t final;

	CreateTableFunctionInformation()
	    : schema(DEFAULT_SCHEMA), or_replace(false) {}
};

struct DropTableFunctionInformation {
	//! Schema name to drop from
	std::string schema;
	//! Table function name to drop
	std::string name;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;

	DropTableFunctionInformation()
	    : schema(DEFAULT_SCHEMA), if_exists(false), cascade(false) {}
};

} // namespace duckdb
