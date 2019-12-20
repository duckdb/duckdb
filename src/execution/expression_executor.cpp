#include "duckdb/execution/expression_executor.hpp"

#include "duckdb/common/types/static_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace duckdb;
using namespace std;

ExpressionExecutor::ExpressionExecutor() : chunk(nullptr) {
}

ExpressionExecutor::ExpressionExecutor(DataChunk *child_chunk) : chunk(child_chunk) {
}

ExpressionExecutor::ExpressionExecutor(DataChunk &child_chunk) : chunk(&child_chunk) {
}

void ExpressionExecutor::Execute(vector<unique_ptr<Expression>> &expressions, DataChunk &result) {
	assert(expressions.size() == result.column_count);
	assert(expressions.size() > 0);
	for (index_t i = 0; i < expressions.size(); i++) {
		ExecuteExpression(*expressions[i], result.data[i]);
		result.heap.MergeHeap(result.data[i].string_heap);
	}
	result.sel_vector = result.data[0].sel_vector;
	result.Verify();
}

void ExpressionExecutor::Execute(vector<Expression *> &expressions, DataChunk &result) {
	assert(expressions.size() == result.column_count);
	assert(expressions.size() > 0);
	for (index_t i = 0; i < expressions.size(); i++) {
		ExecuteExpression(*expressions[i], result.data[i]);
		result.heap.MergeHeap(result.data[i].string_heap);
	}
	result.sel_vector = result.data[0].sel_vector;
	result.Verify();
}

void ExpressionExecutor::Merge(std::vector<std::unique_ptr<Expression>> &expressions) {
	assert(expressions.size() > 0);
	//evaluate all expressions
	for (index_t i = 0; i < expressions.size(); i++) {
		//return if no more true rows
		if (chunk->size() != 0) {
			//evaluate current expression
			Vector intermediate;
			Execute(*expressions[i], intermediate);
			assert(intermediate.type == TypeId::BOOLEAN);
			//if constant and false/null, set count == 0 to fetch the next chunk
			if (intermediate.IsConstant()) {
				if (!intermediate.data[0] || intermediate.nullmask[0]) {
					chunk->data[0].count = 0;
					break;
				}
			} else {
				chunk->SetSelectionVector(intermediate);
			}
		} else {
			break;
		}
	}
}

void ExpressionExecutor::ExecuteExpression(Expression &expr, Vector &result) {
	Vector vector;
	Execute(expr, vector);
	if (chunk) {
		// we have an input chunk: result of this vector should have the same length as input chunk
		// check if the result is a single constant value
		if (vector.count == 1 && (chunk->size() > 1 || vector.sel_vector != chunk->sel_vector)) {
			// have to duplicate the constant value to match the rows in the
			// other columns
			result.count = chunk->size();
			result.sel_vector = chunk->sel_vector;
			VectorOperations::Set(result, vector.GetValue(0));
			result.Move(vector);
		} else if (vector.count != chunk->size()) {
			throw Exception("Computed vector length does not match expected length!");
		}
		assert(vector.sel_vector == chunk->sel_vector);
	}
	assert(result.type == vector.type);
	vector.Move(result);
}

Value ExpressionExecutor::EvaluateScalar(Expression &expr) {
	assert(expr.IsFoldable());
	// use an ExpressionExecutor to execute the expression
	ExpressionExecutor executor;
	Vector result(expr.return_type, true, false);
	executor.ExecuteExpression(expr, result);
	assert(result.count == 1);
	return result.GetValue(0);
}

void ExpressionExecutor::Verify(Expression &expr, Vector &vector) {
	assert(expr.return_type == vector.type);
	vector.Verify();
}

void ExpressionExecutor::Execute(Expression &expr, Vector &result) {
	switch (expr.expression_class) {
	case ExpressionClass::BOUND_REF:
		Execute((BoundReferenceExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_CASE:
		Execute((BoundCaseExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_CAST:
		Execute((BoundCastExpression &)expr, result);
		break;
	case ExpressionClass::COMMON_SUBEXPRESSION:
		Execute((CommonSubExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_COMPARISON:
		Execute((BoundComparisonExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_CONJUNCTION:
		Execute((BoundConjunctionExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_CONSTANT:
		Execute((BoundConstantExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_FUNCTION:
		Execute((BoundFunctionExpression &)expr, result);
		break;
	case ExpressionClass::BOUND_OPERATOR:
		Execute((BoundOperatorExpression &)expr, result);
		break;
	default:
		assert(expr.expression_class == ExpressionClass::BOUND_PARAMETER);
		Execute((BoundParameterExpression &)expr, result);
		break;
	}
	Verify(expr, result);
}
