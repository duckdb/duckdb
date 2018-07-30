
#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalGet : public LogicalOperator {
  public:
	LogicalGet() : LogicalOperator(LogicalOperatorType::GET) {}
	LogicalGet(std::shared_ptr<TableCatalogEntry> table, std::string alias)
	    : LogicalOperator(LogicalOperatorType::GET), table(table),
	      alias(alias) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

	std::shared_ptr<TableCatalogEntry> table;
	std::string alias;
};
} // namespace duckdb