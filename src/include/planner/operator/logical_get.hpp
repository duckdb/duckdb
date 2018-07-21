
#pragma once

#include "planner/logical_operator.hpp"

namespace duckdb {

class LogicalGet : public LogicalOperator {
 public:
 	LogicalGet() : LogicalOperator(LogicalOperatorType::GET) {}
 	LogicalGet(std::shared_ptr<TableCatalogEntry> table) : LogicalOperator(LogicalOperatorType::GET), table(table) {}

	virtual void Accept(LogicalOperatorVisitor *v) override { v->Visit(*this); }

 	std::shared_ptr<TableCatalogEntry> table;
};

}