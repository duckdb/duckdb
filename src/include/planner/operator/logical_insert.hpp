
#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalInsert : public LogicalOperator {
  public:
	LogicalInsert(std::shared_ptr<TableCatalogEntry> table,
	              std::vector<std::unique_ptr<AbstractExpression>> value_list)
	    : LogicalOperator(LogicalOperatorType::INSERT),
	      value_list(move(value_list)), table(table) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	std::vector<std::unique_ptr<AbstractExpression>> value_list;
	std::shared_ptr<TableCatalogEntry> table;
};
} // namespace duckdb
