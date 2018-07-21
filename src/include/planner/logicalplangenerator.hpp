
#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "parser/sql_node_visitor.hpp"

#include "planner/logical_operator.hpp"

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
	
	std::unique_ptr<LogicalOperator> root;
  private:
	Catalog &catalog;
};
}
