//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/operator/logical_update.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalUpdate : public LogicalOperator {
public:
	LogicalUpdate(TableCatalogEntry *table, vector<column_t> columns, vector<unique_ptr<Expression>> expressions)
	    : LogicalOperator(LogicalOperatorType::UPDATE, std::move(expressions)), table(table), columns(columns) {
	}

	TableCatalogEntry *table;
	vector<column_t> columns;

protected:
	void ResolveTypes() override {
		types.push_back(TypeId::BIGINT);
	}
};
} // namespace duckdb
