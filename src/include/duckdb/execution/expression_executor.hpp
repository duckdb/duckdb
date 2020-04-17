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

	//! Execute the ExpressionExecutor and put the result in the result vector; this should only be used for expression
	//! executors with a single expression
	void ExecuteExpression(DataChunk &input, Vector &result);
	//! Execute the ExpressionExecutor and put the result in the result vector; this should only be used for expression
	//! executors with a single expression
	void ExecuteExpression(Vector &result);
	//! Execute the ExpressionExecutor and generate a selection vector from all true values in the result; this should
	//! only be used with a single boolean expression
	idx_t SelectExpression(DataChunk &input, SelectionVector &sel);

	//! Execute the expression with index `expr_idx` and store the result in the result vector
	void ExecuteExpression(idx_t expr_idx, Vector &result);
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
	static unique_ptr<ExpressionState> InitializeState(BoundBetweenExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundCaseExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundCastExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundComparisonExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundConjunctionExpression &expr,
	                                                   ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundConstantExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundFunctionExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundOperatorExpression &expr, ExpressionExecutorState &state);
	static unique_ptr<ExpressionState> InitializeState(BoundParameterExpression &expr, ExpressionExecutorState &state);

	void Execute(Expression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count, Vector &result);

	void Execute(BoundBetweenExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);
	void Execute(BoundCaseExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);
	void Execute(BoundCastExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);
	void Execute(BoundComparisonExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);
	void Execute(BoundConjunctionExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);
	void Execute(BoundConstantExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);
	void Execute(BoundFunctionExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);
	void Execute(BoundOperatorExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);
	void Execute(BoundParameterExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);
	void Execute(BoundReferenceExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             Vector &result);

	//! Execute the (boolean-returning) expression and generate a selection vector with all entries that are "true" in
	//! the result
	idx_t Select(Expression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             SelectionVector *true_sel, SelectionVector *false_sel);
	idx_t DefaultSelect(Expression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel);

	idx_t Select(BoundBetweenExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             SelectionVector *true_sel, SelectionVector *false_sel);
	idx_t Select(BoundComparisonExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             SelectionVector *true_sel, SelectionVector *false_sel);
	idx_t Select(BoundConjunctionExpression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
	             SelectionVector *true_sel, SelectionVector *false_sel);

	//! Verify that the output of a step in the ExpressionExecutor is correct
	void Verify(Expression &expr, Vector &result, idx_t count);

private:
	//! The states of the expression executor; this holds any intermediates and temporary states of expressions
	vector<unique_ptr<ExpressionExecutorState>> states;
};
} // namespace duckdb
