//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// execution/expression_executor.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"
#include "common/types/data_chunk.hpp"

#include "parser/expression/abstract_expression.hpp"
#include "parser/sql_node_visitor.hpp"

#include "execution/physical_operator.hpp"

namespace duckdb {

//! ExpressionExecutor is responsible for executing an arbitrary
//! AbstractExpression and returning a Vector
/*!
    ExpressionExecutor is responsible for executing an arbitrary
   AbstractExpression and returning a Vector. It executes the expressions
   recursively using a visitor pattern.
*/
class ExpressionExecutor : public SQLNodeVisitor {
  public:
	ExpressionExecutor(PhysicalOperatorState *state,
	                   bool scalar_executor = true)
	    : chunk(state ? &state->child_chunk : nullptr), state(state),
	      parent(state ? state->parent : nullptr),
	      scalar_executor(scalar_executor) {}

	ExpressionExecutor(DataChunk &child_chunk,
	                   ExpressionExecutor *parent = nullptr)
	    : chunk(&child_chunk), state(nullptr), parent(parent),
	      scalar_executor(true) {}

	void Reset();

	//! Execute a single abstract expression and store the result in result
	void Execute(AbstractExpression *expr, Vector &result);
	//! Execute the abstract expression, and "logical AND" the result together
	//! with result
	void Merge(AbstractExpression *expr, Vector &result);
	//! Execute the given aggregate expression, and merge the result together
	//! with v
	void Merge(AggregateExpression &expr, Value &v);
	//! Execute the given aggregate expression for the current chunk
	Value Execute(AggregateExpression &expr);

	void Visit(AggregateExpression &expr);
	void Visit(CaseExpression &expr);
	void Visit(CastExpression &expr);
	void Visit(ColumnRefExpression &expr);
	void Visit(ComparisonExpression &expr);
	void Visit(ConjunctionExpression &expr);
	void Visit(ConstantExpression &expr);
	void Visit(FunctionExpression &expr);
	void Visit(GroupRefExpression &expr);
	void Visit(OperatorExpression &expr);
	void Visit(SubqueryExpression &expr);

  private:
	//! Whether or not the ExpressionExecutor is a scalar executor (i.e. output
	//! size = input size), this is true for e.g. expressions in the SELECT
	//! clause without aggregations
	bool scalar_executor;
	//! The data chunk of the current physical operator, used to resolve e.g.
	//! column references
	DataChunk *chunk;

	//! The parent executor of this one, if any. Used for subquery evaluation.
	ExpressionExecutor *parent;

	//! The operator state of the current physical operator, used to resolve
	//! e.g. group-by HT lookups
	PhysicalOperatorState *state;

	//! Intermediate vector that is the result of the previously visited
	//! expression
	Vector vector;
};
} // namespace duckdb
