#include "execution/expression_executor.hpp"

#include "common/vector_operations/vector_operations.hpp"
#include "main/client_context.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Reset() {
	vector.Destroy();
}

void ExpressionExecutor::Execute(DataChunk &result, std::function<Expression *(size_t i)> callback, size_t count) {
	assert(count == result.column_count);
	if (count == 0) {
		return;
	}
	for (size_t i = 0; i < count; i++) {
		auto expression = callback(i);
		if (expression) {
			ExecuteExpression(expression, result.data[i]);
		}
		result.sel_vector = result.data[i].sel_vector;
		result.heap.MergeHeap(result.data[i].string_heap);
	}
}

void ExpressionExecutor::Merge(std::vector<std::unique_ptr<Expression>> &expressions, Vector &result) {
	if (expressions.size() == 0) {
		return;
	}

	ExecuteExpression(expressions[0].get(), result);
	for (size_t i = 1; i < expressions.size(); i++) {
		MergeExpression(expressions[i].get(), result);
	}
}

void ExpressionExecutor::ExecuteExpression(Expression *expr, Vector &result) {
	vector.Destroy();
	VisitExpression(expr);
	if (chunk && scalar_executor) {
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
	if (result.type != vector.type) {
		// cast to the expected type
		VectorOperations::Cast(vector, result);
	} else {
		// types match, only move the data
		vector.Move(result);
	}
}

void ExpressionExecutor::MergeExpression(Expression *expr, Vector &result) {
	vector.Destroy();
	if (result.type != TypeId::BOOLEAN) {
		throw NotImplementedException("Expected a boolean!");
	}
	VisitExpression(expr);
	if (vector.type != TypeId::BOOLEAN) {
		throw NotImplementedException("Expected a boolean!");
	}
	VectorOperations::And(vector, result, result);
}

void ExpressionExecutor::Verify(Expression &expr) {
	expr.stats.Verify(vector);
	// if (chunk) {
	// 	assert(vector.IsConstant() || vector.sel_vector == chunk->sel_vector);
	// }
	assert(expr.return_type == vector.type);
	vector.Verify();
}
