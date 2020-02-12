#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

ExpressionExecutor::ExpressionExecutor() {
}

ExpressionExecutor::ExpressionExecutor(Expression *expression) {
	assert(expression);
	AddExpression(*expression);
}

ExpressionExecutor::ExpressionExecutor(Expression &expression) {
	AddExpression(expression);
}

ExpressionExecutor::ExpressionExecutor(vector<unique_ptr<Expression>> &exprs) {
	assert(exprs.size() > 0);
	for (auto &expr : exprs) {
		AddExpression(*expr);
	}
}

void ExpressionExecutor::AddExpression(Expression &expr) {
	expressions.push_back(&expr);
	auto state = make_unique<ExpressionExecutorState>();
	Initialize(expr, *state);
	states.push_back(move(state));
}

void ExpressionExecutor::Initialize(Expression &expression, ExpressionExecutorState &state) {
	state.root_state = InitializeState(expression, state);
	state.executor = this;
}

void ExpressionExecutor::Execute(DataChunk *input, DataChunk &result) {
	SetChunk(input);

	assert(expressions.size() == result.column_count());
	assert(expressions.size() > 0);
	result.Reset();
	for (index_t i = 0; i < expressions.size(); i++) {
		ExecuteExpression(i, result.data[i]);
	}
	result.sel_vector = result.data[0].sel_vector();
	result.Verify();
}

void ExpressionExecutor::ExecuteExpression(DataChunk &input, Vector &result) {
	SetChunk(&input);
	ExecuteExpression(result);
}

index_t ExpressionExecutor::SelectExpression(DataChunk &input, sel_t result[]) {
	assert(expressions.size() == 1);
	SetChunk(&input);
	return Select(*expressions[0], states[0]->root_state.get(), result);
}

void ExpressionExecutor::ExecuteExpression(Vector &result) {
	assert(expressions.size() == 1);
	ExecuteExpression(0, result);
}

void ExpressionExecutor::ExecuteExpression(index_t expr_idx, Vector &result) {
	assert(expr_idx < expressions.size());
	assert(result.type == expressions[expr_idx]->return_type);
	Execute(*expressions[expr_idx], states[expr_idx]->root_state.get(), result);
}

Value ExpressionExecutor::EvaluateScalar(Expression &expr) {
	assert(expr.IsFoldable());
	// use an ExpressionExecutor to execute the expression
	ExpressionExecutor executor(expr);

	Vector result(expr.return_type);
	executor.ExecuteExpression(result);

	assert(result.vector_type == VectorType::CONSTANT_VECTOR);
	return result.GetValue(0);
}

void ExpressionExecutor::Verify(Expression &expr, Vector &vector) {
	assert(expr.return_type == vector.type);
	vector.Verify();
}

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(Expression &expr, ExpressionExecutorState &state) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_REF:
		return InitializeState((BoundReferenceExpression &)expr, state);
	case ExpressionClass::BOUND_BETWEEN:
		return InitializeState((BoundBetweenExpression &)expr, state);
	case ExpressionClass::BOUND_CASE:
		return InitializeState((BoundCaseExpression &)expr, state);
	case ExpressionClass::BOUND_CAST:
		return InitializeState((BoundCastExpression &)expr, state);
	case ExpressionClass::COMMON_SUBEXPRESSION:
		return InitializeState((CommonSubExpression &)expr, state);
	case ExpressionClass::BOUND_COMPARISON:
		return InitializeState((BoundComparisonExpression &)expr, state);
	case ExpressionClass::BOUND_CONJUNCTION:
		return InitializeState((BoundConjunctionExpression &)expr, state);
	case ExpressionClass::BOUND_CONSTANT:
		return InitializeState((BoundConstantExpression &)expr, state);
	case ExpressionClass::BOUND_FUNCTION:
		return InitializeState((BoundFunctionExpression &)expr, state);
	case ExpressionClass::BOUND_OPERATOR:
		return InitializeState((BoundOperatorExpression &)expr, state);
	case ExpressionClass::BOUND_PARAMETER:
		return InitializeState((BoundParameterExpression &)expr, state);
	default:
		throw NotImplementedException("Attempting to initialize state of expression of unknown type!");
	}
}

void ExpressionExecutor::Execute(Expression &expr, ExpressionState *state, Vector &result) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		Execute((BoundBetweenExpression &)expr, state, result);
		break;
	case ExpressionClass::BOUND_REF:
		Execute((BoundReferenceExpression &)expr, state, result);
		break;
	case ExpressionClass::BOUND_CASE:
		Execute((BoundCaseExpression &)expr, state, result);
		break;
	case ExpressionClass::BOUND_CAST:
		Execute((BoundCastExpression &)expr, state, result);
		break;
	case ExpressionClass::COMMON_SUBEXPRESSION:
		Execute((CommonSubExpression &)expr, state, result);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		Execute((BoundComparisonExpression &)expr, state, result);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		Execute((BoundConjunctionExpression &)expr, state, result);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		Execute((BoundConstantExpression &)expr, state, result);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		Execute((BoundFunctionExpression &)expr, state, result);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		Execute((BoundOperatorExpression &)expr, state, result);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		Execute((BoundParameterExpression &)expr, state, result);
		break;
	default:
		throw NotImplementedException("Attempting to execute expression of unknown type!");
	}
	Verify(expr, result);
}

index_t ExpressionExecutor::Select(Expression &expr, ExpressionState *state, sel_t result[]) {
	assert(expr.return_type == TypeId::BOOL);
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		return Select((BoundBetweenExpression &)expr, state, result);
	case ExpressionClass::BOUND_COMPARISON:
		return Select((BoundComparisonExpression &)expr, state, result);
	case ExpressionClass::BOUND_CONJUNCTION:
		return Select((BoundConjunctionExpression &)expr, state, result);
	default:
		return DefaultSelect(expr, state, result);
	}
}

index_t ExpressionExecutor::DefaultSelect(Expression &expr, ExpressionState *state, sel_t result[]) {
	// generic selection of boolean expression:
	// resolve the true/false expression first
	// then use that to generate the selection vector
	bool intermediate_bools[STANDARD_VECTOR_SIZE];
	Vector intermediate(TypeId::BOOL, (data_ptr_t)intermediate_bools);
	Execute(expr, state, intermediate);

	auto intermediate_result = (bool *)intermediate.GetData();
	if (intermediate.vector_type == VectorType::CONSTANT_VECTOR) {
		// constant result: get the value
		if (intermediate_result[0] && !intermediate.nullmask[0]) {
			// constant true: return everything; we skip filling the selection vector here as it will not be used
			return chunk->size();
		} else {
			// constant false: filter everything
			return 0;
		}
	} else {
		// not a constant value
		index_t result_count = 0;
		VectorOperations::Exec(intermediate, [&](index_t i, index_t k) {
			if (intermediate_result[i] && !intermediate.nullmask[i]) {
				result[result_count++] = i;
			}
		});
		return result_count;
	}
}
