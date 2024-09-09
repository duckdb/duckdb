//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/table_description.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"

namespace duckdb {

class TableDescription {
public:
	TableDescription(const string &database_name, const string &schema_name, const string &table_name)
	    : database(database_name), schema(schema_name), table(table_name) {};

	TableDescription() = delete;

public:
	//! The database of the table.
	string database;
	//! The schema of the table.
	string schema;
	//! The name of the table.
	string table;
	//! True, if the catalog is readonly.
	bool readonly;
	//! The columns of the table.
	vector<ColumnDefinition> columns;
};

} // namespace duckdb
