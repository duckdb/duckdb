//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/planner/bound_tokens.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

//! ExpressionExecutor is responsible for executing a set of expressions and storing the result in a data chunk
class ExpressionExecutor {
public:
	ExpressionExecutor();
	ExpressionExecutor(Expression *expression);
	ExpressionExecutor(Expression &expression);
	ExpressionExecutor(vector<unique_ptr<Expression>> &expressions);

	//! Add an expression to the set of to-be-executed expressions of the executor
	void AddExpression(Expression &expr);

	//! Execute the set of expressions with the given input chunk and store the result in the output chunk
	void Execute(DataChunk *input, DataChunk &result);
	void Execute(DataChunk &input, DataChunk &result) {
		Execute(&input, result);
	}
	void Execute(DataChunk &result) {
		Execute(nullptr, result);
	}

	//! Execute the ExpressionExecutor and put the result in the result vector; this should only be used for expression executors with a single expression
	void ExecuteExpression(DataChunk &input, Vector &result);
	//! Execute the ExpressionExecutor and put the result in the result vector; this should only be used for expression executors with a single expression
	void ExecuteExpression(Vector &result);

	//! Execute the expression with index `expr_idx` and store the result in the result vector
	void ExecuteExpression(index_t expr_idx, Vector &result);
	//! Evaluate a scalar expression and fold it into a single value
	static Value EvaluateScalar(Expression &expr);

	//! Initialize the state of a given expression
	static unique_ptr<ExpressionState> InitializeState(Expression &expr, ExpressionExecutorState &state);

	void SetChunk(DataChunk *chunk) {
		this->chunk = chunk;
	}
	void SetChunk(DataChunk &chunk) {
		SetChunk(&chunk);
	}

	//! The expressions of the executor
	vector<Expression *> expressions;
	//! The data chunk of the current physical operator, used to resolve
	//! column references and determines the output cardinality
	DataChunk *chunk = nullptr;
protected:
	void Initialize(Expression &expr, ExpressionExecutorState &state);

	static unique_ptr<ExpressionState> InitializeState(BoundReferenceExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundCaseExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundCastExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(CommonSubExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundComparisonExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundConjunctionExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundConstantExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundFunctionExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundOperatorExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundParameterExpression &expr, ExpressionExecutorState &state);

	void Execute(Expression &expr, ExpressionState *state, Vector &result);

	void Execute(BoundReferenceExpression &expr, ExpressionState *state, Vector &result);
	void Execute(BoundCaseExpression &expr, ExpressionState *state, Vector &result);
	void Execute(BoundCastExpression &expr, ExpressionState *state, Vector &result);
	void Execute(CommonSubExpression &expr, ExpressionState *state, Vector &result);
	void Execute(BoundComparisonExpression &expr, ExpressionState *state, Vector &result);
	void Execute(BoundConjunctionExpression &expr, ExpressionState *state, Vector &result);
	void Execute(BoundConstantExpression &expr, ExpressionState *state, Vector &result);
	void Execute(BoundFunctionExpression &expr, ExpressionState *state, Vector &result);
	void Execute(BoundOperatorExpression &expr, ExpressionState *state, Vector &result);
	void Execute(BoundParameterExpression &expr, ExpressionState *state, Vector &result);

	//! Verify that the output of a step in the ExpressionExecutor is correct
	void Verify(Expression &expr, Vector &result);
private:
	//! The states of the expression executor; this holds any intermediates and temporary states of expressions
	vector<unique_ptr<ExpressionExecutorState>> states;
	//! The cached result of already-computed Common Subexpression results
	unordered_map<Expression *, unique_ptr<Vector>> cached_cse;
};
} // namespace duckdb
