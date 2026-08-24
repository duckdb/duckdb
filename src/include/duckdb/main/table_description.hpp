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
#include "duckdb/parser/qualified_name.hpp"

namespace duckdb {

class TableDescription {
public:
	explicit TableDescription(QualifiedName qualified_name_p) : qualified_name(std::move(qualified_name_p)) {};

	TableDescription() = delete;

public:
	//! The qualified name of the table (catalog/schema/name).
	QualifiedName qualified_name;
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
