
#include "execution/expression_executor.hpp"
#include "common/types/vector_operations.hpp"

#include "common/exception.hpp"

#include "parser/expression/expression_list.hpp"

#include "execution/operator/physical_aggregate.hpp"
#include "execution/operator/physical_hash_aggregate.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Reset() { vector.Destroy(); }

void ExpressionExecutor::Execute(AbstractExpression *expr, Vector &result) {
	vector.Destroy();
	expr->Accept(this);

	if (result.type != vector.type) {
		// cast to the expected type
		VectorOperations::Cast(vector, result);
	} else {
		// types match, only move the data
		vector.Move(result);
	}
}

void ExpressionExecutor::Merge(AbstractExpression *expr, Vector &result) {
	vector.Destroy();
	if (result.type != TypeId::BOOLEAN) {
		throw NotImplementedException("Expected a boolean!");
	}
	expr->Accept(this);

	if (vector.type != TypeId::BOOLEAN) {
		throw NotImplementedException("Expected a boolean!");
	}
	VectorOperations::And(vector, result, result);
}

Value ExpressionExecutor::Execute(AggregateExpression &expr) {
	vector.Destroy();
	if (expr.type == ExpressionType::AGGREGATE_COUNT_STAR) {
		// COUNT(*)
		// Without FROM clause return "1", else return "count"
		size_t count = chunk.column_count == 0 ? 1 : chunk.count;
		return Value::NumericValue(expr.return_type, count);
	} else if (expr.children.size() > 0) {
		expr.children[0]->Accept(this);
		switch (expr.type) {
		case ExpressionType::AGGREGATE_SUM: {
			return VectorOperations::Sum(vector, expr.stats.CanHaveNull());
		}
		case ExpressionType::AGGREGATE_COUNT: {
			return VectorOperations::Count(vector, expr.stats.CanHaveNull());
		}
		case ExpressionType::AGGREGATE_AVG: {
			return VectorOperations::Sum(vector, expr.stats.CanHaveNull());
		}
		case ExpressionType::AGGREGATE_MIN: {
			return VectorOperations::Min(vector, expr.stats.CanHaveNull());
		}
		case ExpressionType::AGGREGATE_MAX: {
			return VectorOperations::Max(vector, expr.stats.CanHaveNull());
		}
		default:
			throw NotImplementedException("Unsupported aggregate type");
		}
	} else {
		throw NotImplementedException("Aggregate expression without children!");
	}
}

void ExpressionExecutor::Merge(AggregateExpression &expr, Value &result) {
	// if (result.type != expr.return_type) {
	// 	throw NotImplementedException(
	// 	    "Aggregate type does not match value type!");
	// }
	Value v = Execute(expr);
	switch (expr.type) {
	case ExpressionType::AGGREGATE_COUNT_STAR:
	case ExpressionType::AGGREGATE_SUM:
	case ExpressionType::AGGREGATE_COUNT:
	case ExpressionType::AGGREGATE_AVG: {
		Value::Add(result, v, result);
		break;
	}
	case ExpressionType::AGGREGATE_MIN: {
		Value::Min(result, v, result);
		break;
	}
	case ExpressionType::AGGREGATE_MAX: {
		Value::Max(result, v, result);
		break;
	}
	default:
		throw NotImplementedException("Unsupported aggregate type");
	}
}

void ExpressionExecutor::Visit(AggregateExpression &expr) {
	auto state =
	    reinterpret_cast<PhysicalAggregateOperatorState *>(this->state);
	if (!state) {
		throw NotImplementedException("Aggregate node without aggregate state");
	}
	if (state->aggregates.size() == 0) {
		vector.Reference(*state->aggregate_chunk.data[expr.index].get());
	} else {
		Vector v(state->aggregates[expr.index]);
		v.Move(vector);
	}
}

void ExpressionExecutor::Visit(BaseTableRefExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(CastExpression &expr) {
	// resolve the child
	Vector l;
	expr.children[0]->Accept(this);
	if (vector.type == expr.return_type) {
		// NOP cast
		return;
	}
	vector.Move(l);
	// now cast it to the type specified by the cast expression
	vector.Resize(l.count, expr.return_type);
	VectorOperations::Cast(l, vector);
}

void ExpressionExecutor::Visit(ColumnRefExpression &expr) {
	if (expr.index == (size_t)-1) {
		throw Exception("Column Reference not bound!");
	}
	vector.Reference(*chunk.data[expr.index].get());
	expr.stats.Verify(vector);
}

void ExpressionExecutor::Visit(ComparisonExpression &expr) {
	Vector l, r;
	expr.children[0]->Accept(this);
	vector.Move(l);
	expr.children[1]->Accept(this);
	vector.Move(r);
	vector.Resize(std::max(l.count, r.count), TypeId::BOOLEAN);
	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(l, r, vector, expr.stats.CanHaveNull());
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(l, r, vector, expr.stats.CanHaveNull());
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(l, r, vector, expr.stats.CanHaveNull());
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(l, r, vector, expr.stats.CanHaveNull());
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(l, r, vector,
		                                 expr.stats.CanHaveNull());
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(l, r, vector,
		                                    expr.stats.CanHaveNull());
		break;
	case ExpressionType::COMPARE_LIKE:
		throw NotImplementedException("Unimplemented compare: COMPARE_LIKE");
	case ExpressionType::COMPARE_NOTLIKE:
		throw NotImplementedException("Unimplemented compare: COMPARE_NOTLIKE");
	case ExpressionType::COMPARE_IN:
		throw NotImplementedException("Unimplemented compare: COMPARE_IN");
	case ExpressionType::COMPARE_DISTINCT_FROM:
		throw NotImplementedException(
		    "Unimplemented compare: COMPARE_DISTINCT_FROM");
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
}

void ExpressionExecutor::Visit(ConjunctionExpression &expr) {
	if (expr.children.size() != 2) {
		throw Exception("Unsupported conjunction!");
	}
	Vector l, r, result;
	expr.children[0]->Accept(this);
	vector.Move(l);
	expr.children[1]->Accept(this);
	vector.Move(r);
	vector.Resize(std::max(l.count, r.count), TypeId::BOOLEAN);
	switch (expr.type) {
	case ExpressionType::CONJUNCTION_AND:
		VectorOperations::And(l, r, vector, expr.stats.CanHaveNull());
		break;
	case ExpressionType::CONJUNCTION_OR:
		VectorOperations::Or(l, r, vector, expr.stats.CanHaveNull());
		break;
	default:
		throw NotImplementedException("Unknown conjunction type!");
	}
}

void ExpressionExecutor::Visit(ConstantExpression &expr) {
	Vector v(expr.value);
	v.Move(vector);
}

void ExpressionExecutor::Visit(CrossProductExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(FunctionExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(GroupRefExpression &expr) {
	auto state =
	    reinterpret_cast<PhysicalAggregateOperatorState *>(this->state);
	if (!state) {
		throw NotImplementedException("Aggregate node without aggregate state");
	}
	vector.Reference(*state->group_chunk.data[expr.group_index].get());
}

void ExpressionExecutor::Visit(JoinExpression &expr) {
	throw NotImplementedException("");
}

void ExpressionExecutor::Visit(OperatorExpression &expr) {
	if (expr.children.size() == 1) {
		Vector l;
		expr.children[0]->Accept(this);
		vector.Move(l);

		switch (expr.type) {
		default:
			throw NotImplementedException(
			    "Unsupported operator type with 1 child!");
		}
	} else if (expr.children.size() == 2) {
		Vector l, r;
		expr.children[0]->Accept(this);
		vector.Move(l);
		expr.children[1]->Accept(this);
		vector.Move(r);

		vector.Resize(std::max(l.count, r.count));

		switch (expr.type) {
		case ExpressionType::OPERATOR_ADD:
			VectorOperations::Add(l, r, vector, expr.stats.CanHaveNull());
			break;
		case ExpressionType::OPERATOR_SUBTRACT:
			VectorOperations::Subtract(l, r, vector, expr.stats.CanHaveNull());
			break;
		case ExpressionType::OPERATOR_MULTIPLY:
			VectorOperations::Multiply(l, r, vector, expr.stats.CanHaveNull());
			break;
		case ExpressionType::OPERATOR_DIVIDE:
			VectorOperations::Divide(l, r, vector, expr.stats.CanHaveNull());
			break;
		case ExpressionType::OPERATOR_MOD:
			VectorOperations::Modulo(l, r, vector, expr.stats.CanHaveNull());
			break;
		default:
			throw NotImplementedException(
			    "Unsupported operator type with 2 children!");
		}
	} else {
		throw NotImplementedException("operator");
	}
	expr.stats.Verify(vector);
}

void ExpressionExecutor::Visit(CaseExpression &expr) {
	if (expr.children.size() != 3) {
		throw Exception("Cast needs three child nodes");
	}
	Vector check, res_true, res_false;
	expr.children[0]->Accept(this);
	vector.Move(check);
	// TODO: check statistics on check to avoid computing everything
	expr.children[1]->Accept(this);
	vector.Move(res_true);
	expr.children[2]->Accept(this);
	vector.Move(res_false);
	vector.Resize(check.count);
	VectorOperations::Case(check, res_true, res_false, vector);
	expr.stats.Verify(vector);
}

void ExpressionExecutor::Visit(SubqueryExpression &expr) {
	auto &plan = expr.plan;
	auto state = plan->GetOperatorState();

	DataChunk s_chunk;
	plan->InitializeChunk(s_chunk);
	plan->GetChunk(s_chunk, state.get());
	vector.Resize(1);
	vector.count = 1;
	if (s_chunk.count == 0) {
		vector.SetValue(0, Value());
	} else {
		assert(s_chunk.column_count > 0);
		vector.SetValue(0, s_chunk.data[0]->GetValue(0));
	}
	expr.stats.Verify(vector);
}

void ExpressionExecutor::Visit(TableRefExpression &expr) {
	throw NotImplementedException("TableRef");
}
