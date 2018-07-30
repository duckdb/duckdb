
#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"
#include "common/types/data_chunk.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/sql_node_visitor.hpp"

#include "execution/physical_operator.hpp"

namespace duckdb {
class ExpressionExecutor : public SQLNodeVisitor {
  public:
	ExpressionExecutor(DataChunk &chunk, PhysicalOperatorState *state = nullptr)
	    : chunk(chunk), state(state) {}

	void Execute(AbstractExpression *expr, Vector &result);
	void Merge(AbstractExpression *expr, Vector &result);
	void Merge(AggregateExpression &expr, Value &v);

	void Visit(AggregateExpression &expr);
	void Visit(BaseTableRefExpression &expr);
	void Visit(ColumnRefExpression &expr);
	void Visit(ComparisonExpression &expr);
	void Visit(ConjunctionExpression &expr);
	void Visit(ConstantExpression &expr);
	void Visit(CrossProductExpression &expr);
	void Visit(FunctionExpression &expr);
	void Visit(GroupRefExpression &expr);
	void Visit(JoinExpression &expr);
	void Visit(OperatorExpression &expr);
	void Visit(SubqueryExpression &expr);
	void Visit(TableRefExpression &expr);

  private:
	DataChunk &chunk;

	PhysicalOperatorState *state;

	Vector vector;
};
} // namespace duckdb
