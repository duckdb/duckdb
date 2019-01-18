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
		if (state) {
			state->cached_cse.clear();
		}
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

protected:
	void Execute(unique_ptr<Expression> &expr) {
		VisitExpression(expr.get());
	}
	void Execute(Expression *expr) {
		VisitExpression(expr);
	}

	// We don't want to automatically visit children in the ExpressionExecutor, so we replace this method with the empty
	// method
	void VisitExpressionChildren(Expression &expression) override {
	}
	void Visit(AggregateExpression &expr) override {
		throw NotImplementedException("Cannot execute AGGREGATE expression in ExpressionExecutor");
	}
	void Visit(BoundExpression &expr) override;
	void Visit(CaseExpression &expr) override;
	void Visit(CastExpression &expr) override;
	void Visit(ColumnRefExpression &expr) override {
		throw NotImplementedException("Cannot execute COLUMNREF expression in ExpressionExecutor");
	}
	void Visit(BoundColumnRefExpression &expr) override {
		throw NotImplementedException("Cannot execute BOUND COLUMN REF expression in ExpressionExecutor");
	}
	void Visit(CommonSubExpression &expr) override;
	void Visit(ComparisonExpression &expr) override;
	void Visit(ConjunctionExpression &expr) override;
	void Visit(ConstantExpression &expr) override;
	void Visit(DefaultExpression &expr) override {
		throw NotImplementedException("Cannot execute DEFAULT expression in ExpressionExecutor");
	}
	void Visit(BoundFunctionExpression &expr) override;
	void Visit(FunctionExpression &expr) override {
		throw NotImplementedException("Cannot execute FUNCTION expression in ExpressionExecutor");
	}
	void Visit(OperatorExpression &expr) override;
	void Visit(BoundSubqueryExpression &expr) override {
		throw NotImplementedException("Cannot execute BOUND SUBQUERY expression in ExpressionExecutor");
	}
	void Visit(SubqueryExpression &expr) override {
		throw NotImplementedException("Cannot execute SUBQUERY expression in ExpressionExecutor");
	}

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
