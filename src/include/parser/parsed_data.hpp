//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "function/function.hpp"
#include "parser/column_definition.hpp"
#include "parser/constraint.hpp"
#include "parser/statement/select_statement.hpp"

namespace duckdb {

struct CreateTableInformation {
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
	bool temporary = false;

	CreateTableInformation() : schema(DEFAULT_SCHEMA), if_not_exists(false), temporary(false) {
	}
	CreateTableInformation(string schema, string table, vector<ColumnDefinition> columns)
	    : schema(schema), table(table), columns(columns), if_not_exists(false), temporary(false) {
	}
};

struct CreateViewInformation {
	//! Schema name to insert to
	string schema;
	//! Table name to insert to
	string view_name;
	//! Ignore if the entry already exists, instead of failing
	bool replace = false;

	vector<string> aliases;

	unique_ptr<QueryNode> query;

	CreateViewInformation() : schema(DEFAULT_SCHEMA), replace(false) {
	}
	CreateViewInformation(string schema, string view_name) : schema(schema), view_name(view_name), replace(false) {
	}
};

struct DropTableInformation {
	//! Schema name to drop from
	string schema;
	//! Table name to drop
	string table;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;

	DropTableInformation() : schema(DEFAULT_SCHEMA), if_exists(false), cascade(false) {
	}
};

struct DropViewInformation {
	//! Schema name to drop from
	string schema;
	//! Table name to drop
	string view_name;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	DropViewInformation() : schema(DEFAULT_SCHEMA), if_exists(false) {
	}
};

enum class AlterType : uint8_t { INVALID = 0, ALTER_TABLE = 1 };

struct AlterInformation {
	AlterType type;

	AlterInformation(AlterType type) : type(type) {
	}
};

enum class AlterTableType : uint8_t { INVALID = 0, RENAME_COLUMN = 1 };

struct AlterTableInformation : public AlterInformation {
	AlterTableType alter_table_type;
	//! Schema name to alter to
	string schema;
	//! Table name to alter to
	string table;

	AlterTableInformation(AlterTableType type, string schema, string table)
	    : AlterInformation(AlterType::ALTER_TABLE), alter_table_type(type), schema(schema), table(table) {
	}
};

struct RenameColumnInformation : public AlterTableInformation {
	//! Column old name
	string name;
	//! Column new name
	string new_name;

	RenameColumnInformation(string schema, string table, string name, string new_name)
	    : AlterTableInformation(AlterTableType::RENAME_COLUMN, schema, table), name(name), new_name(new_name) {
	}
};

struct CreateSchemaInformation {
	//! Schema name to create
	string schema;
	//! Ignore if the entry already exists, instead of failing
	bool if_not_exists = false;

	CreateSchemaInformation() : if_not_exists(false) {
	}
};

struct DropSchemaInformation {
	//! Schema name to drop
	string schema;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;

	DropSchemaInformation() : if_exists(false), cascade(false) {
	}
};

struct CreateTableFunctionInformation {
	//! Schema name
	string schema;
	//! Function name
	string name;
	//! Replace function if it already exists instead of failing
	bool or_replace = false;
	//! List of return columns
	vector<ColumnDefinition> return_values;
	//! Input arguments
	vector<TypeId> arguments;
	//! Initialize function pointer
	table_function_init_t init;
	//! The function pointer
	table_function_t function;
	//! Final function pointer
	table_function_final_t final;

	CreateTableFunctionInformation() : schema(DEFAULT_SCHEMA), or_replace(false) {
	}
};

struct DropTableFunctionInformation {
	//! Schema name to drop from
	string schema;
	//! Table function name to drop
	string name;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;
	//! Cascade drop (drop all dependents instead of throwing an error if there
	//! are any)
	bool cascade = false;

	DropTableFunctionInformation() : schema(DEFAULT_SCHEMA), if_exists(false), cascade(false) {
	}
};

struct CreateScalarFunctionInformation {
	//! Schema name
	string schema;
	//! Function name
	string name;
	//! Replace function if it already exists instead of failing
	bool or_replace = false;
	//! The main scalar function to execute
	scalar_function_t function;
	//! Function that checks whether or not a set of arguments matches
	matches_argument_function_t matches;
	//! Function that gives the return type of the function given the input
	//! arguments
	get_return_type_function_t return_type;

	CreateScalarFunctionInformation() : schema(DEFAULT_SCHEMA), or_replace(false) {
	}
};

struct CreateIndexInformation {
	////! Index Type (e.g., B+-tree, Skip-List, ...)
	IndexType index_type;
	////! Name of the Index
	string index_name;
	////! If it is an unique index
	bool unique = false;

	//! Ignore if the entry already exists, instead of failing
	bool if_not_exists = false;

	CreateIndexInformation() : if_not_exists(false) {
	}
};

struct DropIndexInformation {
	//! Schema name
	string schema;
	//! Index function name to drop
	string name;
	//! Ignore if the entry does not exist instead of failing
	bool if_exists = false;

	DropIndexInformation() : schema(DEFAULT_SCHEMA), if_exists(false) {
	}
};

struct CopyInformation {
	//! The schema name to copy to/from
	string schema;
	//! The table name to copy to/from
	string table;
	//! The file path to copy to or copy from
	string file_path;
	//! Whether or not this is a copy to file or copy from a file
	bool is_from;
	//! Delimiter to parse
	char delimiter;
	//! Quote to use
	char quote;
	//! Escape character to use
	char escape;
	//! Whether or not the file has a header line
	bool header;
	//! The file format of the external file
	ExternalFileFormat format;
	// List of Columns that will be copied from/to.
	vector<string> select_list;

	CopyInformation()
	    : schema(DEFAULT_SCHEMA), is_from(false), delimiter(','), quote('"'), escape('"'), header(false),
	      format(ExternalFileFormat::CSV) {
	}
};

} // namespace duckdb
