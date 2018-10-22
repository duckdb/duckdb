//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_update.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalUpdate : public LogicalOperator {
  public:
	LogicalUpdate(TableCatalogEntry *table, std::vector<column_t> columns,
	              std::vector<std::unique_ptr<Expression>> expressions)
	    : LogicalOperator(LogicalOperatorType::UPDATE, std::move(expressions)),
	      table(table), columns(columns) {
	}

	virtual void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	TableCatalogEntry *table;
	std::vector<column_t> columns;
};
} // namespace duckdb
