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
	return Select(*expressions[0], states[0]->root_state.get(), nullptr, input.size(), &sel, nullptr);
}

void ExpressionExecutor::ExecuteExpression(Vector &result) {
	assert(expressions.size() == 1);
	ExecuteExpression(0, result);
}

void ExpressionExecutor::ExecuteExpression(idx_t expr_idx, Vector &result) {
	assert(expr_idx < expressions.size());
	assert(result.type == expressions[expr_idx]->return_type);
	Execute(*expressions[expr_idx], states[expr_idx]->root_state.get(), nullptr, chunk ? chunk->size() : 1, result);
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

void ExpressionExecutor::Verify(Expression &expr, Vector &vector, idx_t count) {
	assert(expr.return_type == vector.type);
	vector.Verify(count);
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

void ExpressionExecutor::Execute(Expression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
                                 Vector &result) {
	if (count == 0) {
		return;
	}
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		Execute((BoundBetweenExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_REF:
		Execute((BoundReferenceExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CASE:
		Execute((BoundCaseExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CAST:
		Execute((BoundCastExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		Execute((BoundComparisonExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		Execute((BoundConjunctionExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		Execute((BoundConstantExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		Execute((BoundFunctionExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		Execute((BoundOperatorExpression &)expr, state, sel, count, result);
		break;
	case ExpressionClass::BOUND_PARAMETER:
		Execute((BoundParameterExpression &)expr, state, sel, count, result);
		break;
	default:
		throw NotImplementedException("Attempting to execute expression of unknown type!");
	}
	Verify(expr, result, count);
}

idx_t ExpressionExecutor::Select(Expression &expr, ExpressionState *state, const SelectionVector *sel, idx_t count,
                                 SelectionVector *true_sel, SelectionVector *false_sel) {
	if (count == 0) {
		return 0;
	}
	assert(true_sel || false_sel);
	assert(expr.return_type == TypeId::BOOL);
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_BETWEEN:
		return Select((BoundBetweenExpression &)expr, state, sel, count, true_sel, false_sel);
	case ExpressionClass::BOUND_COMPARISON:
		return Select((BoundComparisonExpression &)expr, state, sel, count, true_sel, false_sel);
	case ExpressionClass::BOUND_CONJUNCTION:
		return Select((BoundConjunctionExpression &)expr, state, sel, count, true_sel, false_sel);
	default:
		return DefaultSelect(expr, state, sel, count, true_sel, false_sel);
	}
}

template <bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
static inline idx_t DefaultSelectLoop(const SelectionVector *bsel, bool *__restrict bdata, nullmask_t &nullmask,
                                      const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
                                      SelectionVector *false_sel) {
	idx_t true_count = 0, false_count = 0;
	for (idx_t i = 0; i < count; i++) {
		auto bidx = bsel->get_index(i);
		auto result_idx = sel->get_index(i);
		if (bdata[bidx] && (NO_NULL || !nullmask[bidx])) {
			if (HAS_TRUE_SEL) {
				true_sel->set_index(true_count++, result_idx);
			}
		} else {
			if (HAS_FALSE_SEL) {
				false_sel->set_index(false_count++, result_idx);
			}
		}
	}
	if (HAS_TRUE_SEL) {
		return true_count;
	} else {
		return count - false_count;
	}
}

template <bool NO_NULL>
static inline idx_t DefaultSelectSwitch(VectorData &idata, const SelectionVector *sel, idx_t count,
                                        SelectionVector *true_sel, SelectionVector *false_sel) {
	if (true_sel && false_sel) {
		return DefaultSelectLoop<NO_NULL, true, true>(idata.sel, (bool *)idata.data, *idata.nullmask, sel, count,
		                                              true_sel, false_sel);
	} else if (true_sel) {
		return DefaultSelectLoop<NO_NULL, true, false>(idata.sel, (bool *)idata.data, *idata.nullmask, sel, count,
		                                               true_sel, false_sel);
	} else {
		assert(false_sel);
		return DefaultSelectLoop<NO_NULL, false, true>(idata.sel, (bool *)idata.data, *idata.nullmask, sel, count,
		                                               true_sel, false_sel);
	}
}

idx_t ExpressionExecutor::DefaultSelect(Expression &expr, ExpressionState *state, const SelectionVector *sel,
                                        idx_t count, SelectionVector *true_sel, SelectionVector *false_sel) {
	// generic selection of boolean expression:
	// resolve the true/false expression first
	// then use that to generate the selection vector
	bool intermediate_bools[STANDARD_VECTOR_SIZE];
	Vector intermediate(TypeId::BOOL, (data_ptr_t)intermediate_bools);
	Execute(expr, state, sel, count, intermediate);

	VectorData idata;
	intermediate.Orrify(count, idata);
	if (!sel) {
		sel = &FlatVector::IncrementalSelectionVector;
	}
	if (idata.nullmask->any()) {
		return DefaultSelectSwitch<false>(idata, sel, count, true_sel, false_sel);
	} else {
		return DefaultSelectSwitch<true>(idata, sel, count, true_sel, false_sel);
	}
}
