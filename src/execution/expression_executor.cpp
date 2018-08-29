
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

	if (chunk && scalar_executor) {
		if (vector.count == 1 &&
		    (chunk->count > 1 || vector.sel_vector != chunk->sel_vector)) {
			// have to duplicate the constant value to match the rows in the
			// other columns
			result.count = chunk->count;
			result.sel_vector = chunk->sel_vector;
			VectorOperations::Set(result, vector.GetValue(0));
			result.Move(vector);
		} else if (vector.count != chunk->count) {
			throw Exception(
			    "Computed vector length does not match expected length!");
		}
		// the expression executor guarantees that
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
		size_t count = chunk->column_count == 0 ? 1 : chunk->count;
		return Value::Numeric(expr.return_type, count);
	} else if (expr.children.size() > 0) {
		expr.children[0]->Accept(this);
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
	case ExpressionType::AGGREGATE_COUNT: {
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
		if (state->aggregate_chunk.column_count &&
		    state->aggregate_chunk.data[expr.index].count) {
			vector.Reference(state->aggregate_chunk.data[expr.index]);
		} else {
			// the subquery scanned no rows, therefore the aggr is empty. return
			// something reasonable depending on aggr type.
			Value val;
			if (expr.type == ExpressionType::AGGREGATE_COUNT ||
			    expr.type == ExpressionType::AGGREGATE_COUNT_STAR) {
				val = Value(0); // ZERO
			} else {
				val = Value(); // NULL
			}
			Vector v(val);
			v.Move(vector);
		}
	} else {
		Vector v(state->aggregates[expr.index]);
		v.Move(vector);
	}
}

void ExpressionExecutor::Visit(CaseExpression &expr) {
	if (expr.children.size() != 3) {
		throw Exception("Cast needs three child nodes");
	}
	Vector check, res_true, res_false;
	expr.children[0]->Accept(this);
	vector.Move(check);
	expr.children[1]->Accept(this);
	vector.Move(res_true);
	expr.children[2]->Accept(this);
	vector.Move(res_false);

	size_t count = max(max(check.count, res_true.count), res_false.count);
	vector.Initialize(res_true.type);
	vector.count = count;
	VectorOperations::Case(check, res_true, res_false, vector);
	expr.stats.Verify(vector);
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
	vector.Initialize(expr.return_type);
	VectorOperations::Cast(l, vector);
}

void ExpressionExecutor::Visit(ColumnRefExpression &expr) {
	size_t cur_depth = expr.depth;
	ExpressionExecutor *cur_exec = this;
	while (cur_depth > 0) {
		cur_exec = cur_exec->parent;
		if (!cur_exec) {
			throw Exception("Unable to find matching parent executor");
		}
		cur_depth--;
	}

	if (expr.index == (size_t)-1) {
		throw Exception("Column Reference not bound!");
	}
	if (expr.index >= cur_exec->chunk->column_count) {
		throw Exception("Column reference index out of range!");
	}
	vector.Reference(cur_exec->chunk->data[expr.index]);
	expr.stats.Verify(vector);
}

void ExpressionExecutor::Visit(ComparisonExpression &expr) {
	Vector l, r;
	expr.children[0]->Accept(this);
	vector.Move(l);
	expr.children[1]->Accept(this);
	vector.Move(r);
	vector.Initialize(TypeId::BOOLEAN);

	switch (expr.type) {
	case ExpressionType::COMPARE_EQUAL:
		VectorOperations::Equals(l, r, vector);
		break;
	case ExpressionType::COMPARE_NOTEQUAL:
		VectorOperations::NotEquals(l, r, vector);
		break;
	case ExpressionType::COMPARE_LESSTHAN:
		VectorOperations::LessThan(l, r, vector);
		break;
	case ExpressionType::COMPARE_GREATERTHAN:
		VectorOperations::GreaterThan(l, r, vector);
		break;
	case ExpressionType::COMPARE_LESSTHANOREQUALTO:
		VectorOperations::LessThanEquals(l, r, vector);
		break;
	case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
		VectorOperations::GreaterThanEquals(l, r, vector);
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
	vector.Initialize(TypeId::BOOLEAN);
	switch (expr.type) {
	case ExpressionType::CONJUNCTION_AND:
		VectorOperations::And(l, r, vector);
		break;
	case ExpressionType::CONJUNCTION_OR:
		VectorOperations::Or(l, r, vector);
		break;
	default:
		throw NotImplementedException("Unknown conjunction type!");
	}
}

void ExpressionExecutor::Visit(ConstantExpression &expr) {
	Vector v(expr.value);
	v.Move(vector);
}

void ExpressionExecutor::Visit(FunctionExpression &expr) {
	if (expr.func_name == "abs") {
		Vector l;
		expr.children[0]->Accept(this);
		vector.Move(l);
		vector.Initialize(l.type);
		VectorOperations::Abs(l, vector);
		expr.stats.Verify(vector);
		return;
	}

	throw NotImplementedException("Function not implemented");
}

void ExpressionExecutor::Visit(GroupRefExpression &expr) {
	auto state =
	    reinterpret_cast<PhysicalAggregateOperatorState *>(this->state);
	if (!state) {
		throw NotImplementedException("Aggregate node without aggregate state");
	}
	vector.Reference(state->group_chunk.data[expr.group_index]);
}

void ExpressionExecutor::Visit(OperatorExpression &expr) {
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN) {
		if (expr.children.size() < 2) {
			throw Exception("IN needs at least two children");
		}
		Vector l, comp_res;
		// eval left side
		expr.children[0]->Accept(this);
		vector.Move(l);

		// init result to false
		Vector result;
		result.Initialize(TypeId::BOOLEAN);
		result.count = l.count;
		VectorOperations::Set(result, Value(false));

		// init comparision result once
		comp_res.Initialize(TypeId::BOOLEAN);
		comp_res.count = l.count;

		// for every child, or result of comparision with left to overall result
		for (size_t child = 1; child < expr.children.size(); child++) {
			expr.children[child]->Accept(this);
			VectorOperations::Equals(l, vector, comp_res);
			vector.Destroy();
			Vector temp_result;
			temp_result.Initialize(TypeId::BOOLEAN);
			result.Copy(temp_result);
			VectorOperations::Or(temp_result, comp_res, result);
			// early abort
			if (Value::Equals(VectorOperations::Min(result), Value(true)) &&
			    Value::Equals(VectorOperations::Max(result), Value(true))) {
				break;
			}
		}
		result.Move(vector);
		expr.stats.Verify(vector);
		return;
	}
	if (expr.children.size() == 1) {
		expr.children[0]->Accept(this);
		switch (expr.type) {
		case ExpressionType::OPERATOR_EXISTS:
			// the subquery in the only child will already create the correct
			// result
			break;
		case ExpressionType::OPERATOR_NOT: {
			Vector l;

			vector.Move(l);
			vector.Initialize(l.type);

			VectorOperations::Not(l, vector);
			break;
		}
		case ExpressionType::OPERATOR_IS_NULL: {
			Vector l;
			vector.Move(l);
			vector.Initialize(TypeId::BOOLEAN);
			VectorOperations::IsNull(l, vector);
			break;
		}
		case ExpressionType::OPERATOR_IS_NOT_NULL: {
			Vector l;
			vector.Move(l);
			vector.Initialize(TypeId::BOOLEAN);
			VectorOperations::IsNotNull(l, vector);
			break;
		}
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

		vector.Initialize(l.type);

		switch (expr.type) {
		case ExpressionType::OPERATOR_ADD:
			VectorOperations::Add(l, r, vector);
			break;
		case ExpressionType::OPERATOR_SUBTRACT:
			VectorOperations::Subtract(l, r, vector);
			break;
		case ExpressionType::OPERATOR_MULTIPLY:
			VectorOperations::Multiply(l, r, vector);
			break;
		case ExpressionType::OPERATOR_DIVIDE:
			VectorOperations::Divide(l, r, vector);
			break;
		case ExpressionType::OPERATOR_MOD:
			VectorOperations::Modulo(l, r, vector);
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

void ExpressionExecutor::Visit(SubqueryExpression &expr) {
	auto &plan = expr.plan;
	DataChunk *old_chunk = chunk;
	DataChunk row_chunk;
	chunk = &row_chunk;
	auto types = old_chunk->GetTypes();
	row_chunk.Initialize(types, true);
	row_chunk.count = 1;

	vector.Initialize(expr.return_type);
	vector.count = old_chunk->count;
	vector.sel_vector = old_chunk->sel_vector;
	for (size_t c = 0; c < old_chunk->column_count; c++) {
		row_chunk.data[c].count = 1;
	}

	for (size_t r = 0; r < old_chunk->count; r++) {
		for (size_t c = 0; c < old_chunk->column_count; c++) {
			row_chunk.data[c].SetValue(0, old_chunk->data[c].GetValue(r));
		}
		auto state = plan->GetOperatorState(this);
		DataChunk s_chunk;
		plan->InitializeChunk(s_chunk);
		plan->GetChunk(s_chunk, state.get());
		if (!expr.exists) {
			if (s_chunk.count == 0) {
				vector.SetValue(r, Value());
			} else {
				assert(s_chunk.column_count > 0);
				vector.SetValue(r, s_chunk.data[0].GetValue(0));
			}
		} else {
			vector.SetValue(r, Value::BOOLEAN(s_chunk.count != 0));
		}
	}
	chunk = old_chunk;

	expr.stats.Verify(vector);
}
