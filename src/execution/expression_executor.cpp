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
	for (idx_t i = 0; i < expressions.size(); i++) {
		ExecuteExpression(i, result.data[i]);
	}
	result.SetCardinality(input ? input->size() : 1);
	result.Verify();
}

void ExpressionExecutor::ExecuteExpression(DataChunk &input, Vector &result) {
	SetChunk(&input);
	ExecuteExpression(result);
}

idx_t ExpressionExecutor::SelectExpression(DataChunk &input, SelectionVector &sel) {
	assert(expressions.size() == 1);
	SetChunk(&input);
	SelectionVector false_sel(STANDARD_VECTOR_SIZE);
	return Select(*expressions[0], states[0]->root_state.get(), input.size(), sel, false_sel);
}

void ExpressionExecutor::ExecuteExpression(Vector &result) {
	assert(expressions.size() == 1);
	ExecuteExpression(0, result);
}

void ExpressionExecutor::ExecuteExpression(idx_t expr_idx, Vector &result) {
	assert(expr_idx < expressions.size());
	assert(result.type == expressions[expr_idx]->return_type);
	Execute(*expressions[expr_idx], states[expr_idx]->root_state.get(), result, chunk ? chunk->size() : 1);
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
	vector.Verify(chunk ? chunk->size() : 1);
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

void ExpressionExecutor::Execute(Expression &expr, ExpressionState *state, Vector &result, idx_t count) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		Execute((BoundBetweenExpression &)expr, state, result, count);
		break;
	case ExpressionClass::BOUND_REF:
		Execute((BoundReferenceExpression &)expr, state, result, count);
		break;
	case ExpressionClass::BOUND_CASE:
		Execute((BoundCaseExpression &)expr, state, result, count);
		break;
	case ExpressionClass::BOUND_CAST:
		Execute((BoundCastExpression &)expr, state, result, count);
		break;
	case ExpressionClass::COMMON_SUBEXPRESSION:
		Execute((CommonSubExpression &)expr, state, result, count);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		Execute((BoundComparisonExpression &)expr, state, result, count);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		Execute((BoundConjunctionExpression &)expr, state, result, count);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		Execute((BoundConstantExpression &)expr, state, result, count);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		Execute((BoundFunctionExpression &)expr, state, result, count);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		Execute((BoundOperatorExpression &)expr, state, result, count);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		Execute((BoundParameterExpression &)expr, state, result, count);
		break;
	default:
		throw NotImplementedException("Attempting to execute expression of unknown type!");
	}
	Verify(expr, result);
}

idx_t ExpressionExecutor::Select(Expression &expr, ExpressionState *state, idx_t count, SelectionVector &true_sel, SelectionVector &false_sel) {
	assert(expr.return_type == TypeId::BOOL);
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		return Select((BoundBetweenExpression &)expr, state, count, true_sel, false_sel);
	case ExpressionClass::BOUND_COMPARISON:
		return Select((BoundComparisonExpression &)expr, state, count, true_sel, false_sel);
	case ExpressionClass::BOUND_CONJUNCTION:
		return Select((BoundConjunctionExpression &)expr, state, count, true_sel, false_sel);
	default:
		return DefaultSelect(expr, state, count, true_sel, false_sel);
	}
}

idx_t ExpressionExecutor::DefaultSelect(Expression &expr, ExpressionState *state, idx_t count, SelectionVector &true_sel, SelectionVector &false_sel) {
	// generic selection of boolean expression:
	// resolve the true/false expression first
	// then use that to generate the selection vector
	bool intermediate_bools[STANDARD_VECTOR_SIZE];
	Vector intermediate(TypeId::BOOL, (data_ptr_t)intermediate_bools);
	Execute(expr, state, intermediate, count);

	switch(intermediate.vector_type) {
	case VectorType::CONSTANT_VECTOR: {
		// constant result: get the value
		auto idata = ConstantVector::GetData<bool>(intermediate);
		if (*idata && !ConstantVector::IsNull(intermediate)) {
			// constant true: return everything; we skip filling the selection vector here as it will not be used
			return count;
		} else {
			// constant false: filter everything
			return 0;
		}
	}
	default:
		intermediate.Normalify(count);
		idx_t true_count = 0, false_count = 0;
		auto idata = FlatVector::GetData<bool>(intermediate);
		auto &nullmask = FlatVector::Nullmask(intermediate);
		if (nullmask.any()) {
			for(idx_t i = 0; i < count; i++) {
				if (idata[i] && !nullmask[i]) {
					true_sel.set_index(true_count++, i);
				} else {
					false_sel.set_index(false_count++, i);
				}
			}
		} else {
			for(idx_t i = 0; i < count; i++) {
				if (idata[i]) {
					true_sel.set_index(true_count++, i);
				} else {
					false_sel.set_index(false_count++, i);
				}
			}
		}
		return true_count;
	}
}
