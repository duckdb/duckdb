#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"
#include "execution/operator/aggregate/physical_aggregate.hpp"
#include "execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "parser/expression/aggregate_expression.hpp"

using namespace duckdb;
using namespace std;

Value ExpressionExecutor::ExecuteAggregate(AggregateExpression &expr) {
	vector.Destroy();
	if (expr.type == ExpressionType::AGGREGATE_COUNT_STAR) {
		// COUNT(*)
		// Without FROM clause return "1", else return "count"
		size_t count = chunk->column_count == 0 ? 1 : chunk->size();
		return Value::Numeric(expr.return_type, count);
	} else if (expr.children.size() > 0) {
		expr.children[0]->Accept(this);
		if (vector.count == 1 && vector.count < chunk->size()) {
			vector.count = chunk->size();
			VectorOperations::Set(vector, vector.GetValue(0));
		}
		switch (expr.type) {
		case ExpressionType::AGGREGATE_SUM: {
			return VectorOperations::Sum(vector);
		}
		case ExpressionType::AGGREGATE_COUNT: {
			return VectorOperations::Count(vector);
		}
		case ExpressionType::AGGREGATE_MIN: {
			return VectorOperations::Min(vector);
		}
		case ExpressionType::AGGREGATE_MAX: {
			return VectorOperations::Max(vector);
		}
		case ExpressionType::AGGREGATE_FIRST: {
			if (vector.count > 0) {
				return vector.GetValue(0).CastAs(expr.return_type);
			} else {
				return Value().CastAs(expr.return_type);
			}
		}
		default:
			throw NotImplementedException("Unsupported aggregate type");
		}
	} else {
		throw NotImplementedException("Aggregate expression without children!");
	}
}

static bool IsScalarAggr(Expression *expr) {
	if (expr->type == ExpressionType::COLUMN_REF || expr->type == ExpressionType::GROUP_REF ||
	    expr->type == ExpressionType::AGGREGATE_COUNT_STAR) {
		return false;
	}
	for (auto &child : expr->children) {
		if (!IsScalarAggr(child.get())) {
			return false;
		}
	}
	return true;
}

unique_ptr<Expression> ExpressionExecutor::Visit(AggregateExpression &expr) {
	auto state = reinterpret_cast<PhysicalAggregateOperatorState *>(this->state);
	if (!state) {
		throw NotImplementedException("Aggregate node without aggregate state");
	}
	if (state->aggregates.size() == 0) {
		if (state->aggregate_chunk.column_count && state->aggregate_chunk.data[expr.index].count) {
			vector.Reference(state->aggregate_chunk.data[expr.index]);
		} else {
			if (IsScalarAggr(&expr)) { // even if we do not scan rows, we can
				                       // still have a result e.g. MAX(42)
				ExecuteAggregate(expr);
			} else {
				// the subquery scanned no rows, therefore the aggr is empty.
				// return something reasonable depending on aggr type.
				Value val;
				if (expr.type == ExpressionType::AGGREGATE_COUNT || expr.type == ExpressionType::AGGREGATE_COUNT_STAR) {
					val = Value(0).CastAs(expr.return_type); // ZERO
				} else {
					val = Value().CastAs(expr.return_type); // NULL
				}

				Vector v(val);
				v.Move(vector);
			}
		}
	} else {
		Vector v(state->aggregates[expr.index]);
		v.Move(vector);
	}
	expr.stats.Verify(vector);
	return nullptr;
}
