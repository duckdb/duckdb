//===----------------------------------------------------------------------===//
//                         DuckDB
//
// execution/expression_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/types/data_chunk.hpp"
#include "execution/physical_operator.hpp"
#include "parser/expression.hpp"
#include "parser/sql_node_visitor.hpp"

namespace duckdb {

class ClientContext;

//! ExpressionExecutor is responsible for executing an arbitrary
//! Expression and returning a Vector
/*!
    ExpressionExecutor is responsible for executing an arbitrary
   Expression and returning a Vector. It executes the expressions
   recursively using a visitor pattern.
*/
class ExpressionExecutor : public SQLNodeVisitor {
public:
	ExpressionExecutor(PhysicalOperatorState *state, ClientContext &context, bool scalar_executor = true)
	    : context(context), scalar_executor(scalar_executor), chunk(state ? &state->child_chunk : nullptr),
	      parent(state ? state->parent : nullptr), state(state) {
	}

	ExpressionExecutor(DataChunk &child_chunk, ClientContext &context, ExpressionExecutor *parent = nullptr)
	    : context(context), scalar_executor(true), chunk(&child_chunk), parent(parent), state(nullptr) {
	}

	void Reset();

	void Execute(DataChunk &result, std::function<Expression *(size_t i)> callback, size_t count);
	//! Executes a set of expressions and stores them in the result chunk
	void Execute(vector<unique_ptr<Expression>> &expressions, DataChunk &result) {
		Execute(result, [&](size_t i) { return expressions[i].get(); }, expressions.size());
	}
	//! Executes a set of column expresions and merges them using the logical
	//! AND operator
	void Merge(std::vector<std::unique_ptr<Expression>> &expressions, Vector &result);

	//! Execute a single abstract expression and store the result in result
	void ExecuteExpression(Expression *expr, Vector &result);
	//! Execute the abstract expression, and "logical AND" the result together
	//! with result
	void MergeExpression(Expression *expr, Vector &result);
	//! Verify that the output of a step in the ExpressionExecutor is correct
	void Verify(Expression &expr);

	unique_ptr<Expression> Visit(AggregateExpression &expr) {
		throw NotImplementedException("Cannot execute AGGREGATE expression in ExpressionExecutor");
	}
	unique_ptr<Expression> Visit(CaseExpression &expr);
	unique_ptr<Expression> Visit(CastExpression &expr);
	unique_ptr<Expression> Visit(ColumnRefExpression &expr);
	unique_ptr<Expression> Visit(ComparisonExpression &expr);
	unique_ptr<Expression> Visit(ConjunctionExpression &expr);
	unique_ptr<Expression> Visit(ConstantExpression &expr);
	unique_ptr<Expression> Visit(DefaultExpression &expr) {
		throw NotImplementedException("Cannot execute DEFAULT expression in ExpressionExecutor");
	}
	unique_ptr<Expression> Visit(FunctionExpression &expr);
	unique_ptr<Expression> Visit(GroupRefExpression &expr);
	unique_ptr<Expression> Visit(OperatorExpression &expr);
	unique_ptr<Expression> Visit(SubqueryExpression &expr);

private:
	ClientContext &context;

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
