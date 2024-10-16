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

struct TableDescription {
public:
	//! The schema of the table
	string schema;
	//! The table name of the table
	string table;
	//! The columns of the table
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
