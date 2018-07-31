
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

	virtual void Visit(SelectStatement &statement) override;
	virtual void Visit(InsertStatement &statement) override;

	virtual void Visit(BaseTableRefExpression &expr) override;
	virtual void Visit(ComparisonExpression &expr) override;
	virtual void Visit(ConjunctionExpression &expr) override;
	virtual void Visit(JoinExpression &expr) override;
	virtual void Visit(OperatorExpression &expr) override;
	virtual void Visit(SubqueryExpression &expr) override;

	void Print() { root->Print(); }

	std::unique_ptr<LogicalOperator> root;

  private:
	Catalog &catalog;
};
} // namespace duckdb
