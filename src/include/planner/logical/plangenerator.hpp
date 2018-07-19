
#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"
#include "common/sql_node_visitor.hpp"

#include "planner/logical/operators.hpp"

namespace duckdb {
class LogicalPlanGenerator : public SQLNodeVisitor {
  public:
  	LogicalPlanGenerator(Catalog &catalog) : catalog(catalog) {}

	void Visit(SelectStatement &statement);

	void Visit(BaseTableRefExpression &expr);
	void Visit(JoinExpression &expr);
	void Visit(SubqueryExpression &expr);

	void Print() {
		root->Print();
	}
  private:
	Catalog &catalog;
	
	std::unique_ptr<LogicalOperator> root;
};
}
