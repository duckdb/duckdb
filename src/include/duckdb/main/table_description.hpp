//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/table_description.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/column_definition.hpp"
#include "duckdb/common/identifier.hpp"

namespace duckdb {

class TableDescription {
public:
	TableDescription(Identifier database_name, Identifier schema_name, Identifier table_name)
	    : database(std::move(database_name)), schema(std::move(schema_name)), table(std::move(table_name)) {};

	TableDescription() = delete;

public:
	//! The database of the table.
	Identifier database;
	//! The schema of the table.
	Identifier schema;
	//! The name of the table.
	Identifier table;
	//! True, if the catalog is readonly.
	bool readonly;
	//! The columns of the table.
	vector<ColumnDefinition> columns;

public:
	idx_t PhysicalColumnCount() const {
		idx_t count = 0;
		for (auto &column : columns) {
			if (column.Generated()) {
				continue;
			}
			count++;
		}
		D_ASSERT(count != 0);
		return count;
	}
};

} // namespace duckdb
