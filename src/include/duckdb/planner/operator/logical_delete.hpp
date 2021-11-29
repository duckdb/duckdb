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
	    : LogicalOperator(LogicalOperatorType::LOGICAL_DELETE), table(table) {
	}

	TableCatalogEntry *table;

protected:
	void ResolveTypes() override {
		types.emplace_back(LogicalType::BIGINT);
	}
};
} // namespace duckdb
