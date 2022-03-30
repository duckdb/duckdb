//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_delete.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class LogicalDelete : public LogicalOperator {
public:
	explicit LogicalDelete(TableCatalogEntry *table)
	    : LogicalOperator(LogicalOperatorType::LOGICAL_DELETE), table(table), table_index(0), return_chunk(false) {
	}

	TableCatalogEntry *table;
	idx_t table_index;
	bool return_chunk;

protected:
	vector<ColumnBinding> GetColumnBindings() override {
		if (return_chunk) {
			return GenerateColumnBindings(table_index, table->columns.size());
		}
		return {ColumnBinding(0, 0)};
	}

	void ResolveTypes() override {
		if (return_chunk) {
			types = table->GetTypes();
		} else {
			types.emplace_back(LogicalType::BIGINT);
		}
	}
};
} // namespace duckdb
