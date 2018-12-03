//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/operator/logical_delete.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalDelete : public LogicalOperator {
  public:
	LogicalDelete(TableCatalogEntry *table)
	    : LogicalOperator(LogicalOperatorType::DELETE), table(table) {
	}

	void Accept(LogicalOperatorVisitor *v) override {
		v->Visit(*this);
	}

	TableCatalogEntry *table;
};
} // namespace duckdb
