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
	LogicalDelete(TableCatalogEntry *table) : LogicalOperator(LogicalOperatorType::DELETE), table(table) {
	}

	TableCatalogEntry *table;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::INT64);
	}
};
} // namespace duckdb
