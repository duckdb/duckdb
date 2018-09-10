//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/operator/logical_delete.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalDelete : public LogicalOperator {
  public:
	LogicalDelete(TableCatalogEntry *table)
	    : LogicalOperator(LogicalOperatorType::DELETE), table(table) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	TableCatalogEntry *table;
};
} // namespace duckdb
