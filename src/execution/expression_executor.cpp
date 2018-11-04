
#include "execution/expression_executor.hpp"
#include "common/vector_operations/vector_operations.hpp"

#include "common/exception.hpp"

#include "main/client_context.hpp"

#include "parser/expression/list.hpp"

#include "common/value_operations/value_operations.hpp"
#include "execution/operator/physical_aggregate.hpp"
#include "execution/operator/physical_hash_aggregate.hpp"

using namespace duckdb;
using namespace std;

void ExpressionExecutor::Reset() {
	vector.Destroy();
}

void ExpressionExecutor::Execute(DataChunk &result,
                                 std::function<Expression *(size_t i)> callback,
                                 size_t count) {
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

void ExpressionExecutor::Merge(
    std::vector<std::unique_ptr<Expression>> &expressions, Vector &result) {
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
	expr->Accept(this);

	if (chunk && scalar_executor) {
		if (vector.count == 1 &&
		    (chunk->size() > 1 || vector.sel_vector != chunk->sel_vector)) {
			// have to duplicate the constant value to match the rows in the
			// other columns
			result.count = chunk->size();
			result.sel_vector = chunk->sel_vector;
			VectorOperations::Set(result, vector.GetValue(0));
			result.Move(vector);
		} else if (vector.count != chunk->size()) {
			throw Exception(
			    "Computed vector length does not match expected length!");
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
	expr->Accept(this);

	if (vector.type != TypeId::BOOLEAN) {
		throw NotImplementedException("Expected a boolean!");
	}
	VectorOperations::And(vector, result, result);
}

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

void ExpressionExecutor::MergeAggregate(AggregateExpression &expr,
                                        Value &result) {
	// if (result.type != expr.return_type) {
	// 	throw NotImplementedException(
	// 	    "Aggregate type does not match value type!");
	// }
	Value v = ExecuteAggregate(expr);
	switch (expr.type) {
	case ExpressionType::AGGREGATE_COUNT_STAR:
	case ExpressionType::AGGREGATE_SUM:
	case ExpressionType::AGGREGATE_COUNT: {
		ValueOperations::Add(result, v, result);
		break;
	}
	case ExpressionType::AGGREGATE_MIN: {
		ValueOperations::Min(result, v, result);
		break;
	}
	case ExpressionType::AGGREGATE_MAX: {
		ValueOperations::Max(result, v, result);
		break;
	}
	// we don't have to merge since the first chunk already set the result
	case ExpressionType::AGGREGATE_FIRST:
		break;
	default:
		throw NotImplementedException("Unsupported aggregate type");
	}
}

static bool IsScalarAggr(Expression *expr) {
	if (expr->type == ExpressionType::COLUMN_REF ||
	    expr->type == ExpressionType::GROUP_REF ||
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
			if (IsScalarAggr(&expr)) { // even if we do not scan rows, we can
				                       // still have a result e.g. MAX(42)
				ExecuteAggregate(expr);
			} else {
				// the subquery scanned no rows, therefore the aggr is empty.
				// return something reasonable depending on aggr type.
				Value val;
				if (expr.type == ExpressionType::AGGREGATE_COUNT ||
				    expr.type == ExpressionType::AGGREGATE_COUNT_STAR) {
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

	vector.Initialize(res_true.type);
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
	expr.stats.Verify(vector);
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
		VectorOperations::Like(l, r, vector);
		break;
	case ExpressionType::COMPARE_NOTLIKE:
		VectorOperations::NotLike(l, r, vector);
		break;
	case ExpressionType::COMPARE_DISTINCT_FROM:
		throw NotImplementedException(
		    "Unimplemented compare: COMPARE_DISTINCT_FROM");
	default:
		throw NotImplementedException("Unknown comparison type!");
	}
	expr.stats.Verify(vector);
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
	expr.stats.Verify(vector);
}

void ExpressionExecutor::Visit(ConstantExpression &expr) {
	vector.Reference(expr.value);
	expr.stats.Verify(vector);
}

void ExpressionExecutor::Visit(DefaultExpression &expr) {
	throw NotImplementedException(
	    "Cannot execute DEFAULT expression in ExpressionExecutor");
}

void ExpressionExecutor::Visit(FunctionExpression &expr) {
	assert(expr.bound_function);

	auto arguments = unique_ptr<Vector[]>(new Vector[expr.children.size()]);
	for (size_t i = 0; i < expr.children.size(); i++) {
		expr.children[i]->Accept(this);
		vector.Move(arguments[i]);
	}
	vector.Destroy();
	expr.bound_function->function(arguments.get(), expr.children.size(),
	                              vector);
	if (vector.type != expr.return_type) {
		throw TypeMismatchException(expr.return_type, vector.type,
		                            "expected function to return the former "
		                            "but the function returned the latter");
	}
}

void ExpressionExecutor::Visit(GroupRefExpression &expr) {
	auto state =
	    reinterpret_cast<PhysicalAggregateOperatorState *>(this->state);
	if (!state) {
		throw NotImplementedException("Aggregate node without aggregate state");
	}
	vector.Reference(state->group_chunk.data[expr.group_index]);
	expr.stats.Verify(vector);
}

void ExpressionExecutor::Visit(OperatorExpression &expr) {
	// special handling for special snowflake 'IN'
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN) {
		if (expr.children.size() < 2) {
			throw Exception("IN needs at least two children");
		}
		Vector l;
		// eval left side
		expr.children[0]->Accept(this);
		vector.Move(l);

		// init result to false
		Vector result;
		result.Initialize(TypeId::BOOLEAN);
		result.count = l.count;
		result.sel_vector = l.sel_vector;
		VectorOperations::Set(result, Value(false));

		// FIXME this is very similar to the visit method of subqueries,
		// could/should be merged
		if (expr.children[1]->type == ExpressionType::SELECT_SUBQUERY) {
			assert(expr.children.size() == 2);

			auto subquery =
			    reinterpret_cast<SubqueryExpression *>(expr.children[1].get());
			assert(subquery->subquery_type == SubqueryType::IN);

			DataChunk *old_chunk = chunk;
			DataChunk row_chunk;
			chunk = &row_chunk;
			auto types = old_chunk->GetTypes();
			auto &plan = subquery->plan;

			// row chunk is used to handle correlated subqueries
			row_chunk.Initialize(types, true);
			for (size_t c = 0; c < old_chunk->column_count; c++) {
				row_chunk.data[c].count = 1;
			}

			// l could be scalar, make sure we have the same counts everywhere
			if (l.count != old_chunk->size()) {
				l.count = old_chunk->size();
				// replicate
				VectorOperations::Set(l, l.GetValue(0));
				result.count = old_chunk->size();
			}

			assert(l.count == old_chunk->size());
			assert(result.count == old_chunk->size());

			for (size_t r = 0; r < old_chunk->size(); r++) {
				for (size_t c = 0; c < old_chunk->column_count; c++) {
					row_chunk.data[c].SetValue(0,
					                           old_chunk->data[c].GetValue(r));
				}
				auto state = plan->GetOperatorState(this);
				DataChunk s_chunk;
				plan->InitializeChunk(s_chunk);
				plan->GetChunk(context, s_chunk, state.get());

				// easy case, subquery yields no result, so result is false
				if (s_chunk.size() == 0) {
					result.SetValue(r, Value(false));
					continue;
				}
				if (s_chunk.column_count != 1) {
					throw Exception(
					    "IN subquery needs to return exactly one column");
				}
				assert(s_chunk.column_count == 1);
				Value res = Value(false);
				Vector lval_vec(l.GetValue(r));
				Vector &rval_vec = s_chunk.GetVector(0);
				Vector comp_res;
				comp_res.Initialize(TypeId::BOOLEAN);
				comp_res.count = rval_vec.count;
				// if there is any true in comp_res the IN returns true
				VectorOperations::Equals(lval_vec, rval_vec, comp_res);
				// if we find any match, IN is true
				if (ValueOperations::Equals(VectorOperations::AnyTrue(comp_res),
				                            Value(true))) {
					result.SetValue(r, Value(true));
				} else {
					// if not, but there are some NULLs in the rhs, its a NULL
					if (comp_res.nullmask.any()) {
						result.SetValue(r, Value());
						// otherwise no
					} else {
						result.SetValue(r, Value(false));
					}
				}
			}
			chunk = old_chunk;

		} else {
			// in rhs is a list of constants
			// for every child, OR the result of the comparision with the left
			// to get the overall result.
			for (size_t child = 1; child < expr.children.size(); child++) {
				Vector comp_res(TypeId::BOOLEAN, true, false);
				expr.children[child]->Accept(this);
				VectorOperations::Equals(l, vector, comp_res);
				vector.Destroy();
				if (child == 1) {
					// first child: move to result
					comp_res.Move(result);
				} else {
					// otherwise OR together
					Vector new_result(TypeId::BOOLEAN, true, false);
					VectorOperations::Or(result, comp_res, new_result);
					new_result.Move(result);
				}
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
	if (old_chunk) {
		chunk = &row_chunk;
		auto types = old_chunk->GetTypes();
		row_chunk.Initialize(types, true);
		for (size_t c = 0; c < old_chunk->column_count; c++) {
			row_chunk.data[c].count = 1;
		}
	}
	vector.Initialize(expr.return_type);
	vector.count = old_chunk ? old_chunk->size() : 1;
	vector.sel_vector = old_chunk ? old_chunk->sel_vector : nullptr;

	for (size_t r = 0; r < vector.count; r++) {
		if (old_chunk) {
			for (size_t c = 0; c < row_chunk.column_count; c++) {
				row_chunk.data[c].SetValue(0, old_chunk->data[c].GetValue(r));
			}
		}
		auto state = plan->GetOperatorState(this);
		DataChunk s_chunk;
		plan->InitializeChunk(s_chunk);
		plan->GetChunk(context, s_chunk, state.get());

		switch (expr.subquery_type) {
		case SubqueryType::DEFAULT:
			if (s_chunk.size() == 0) {
				vector.SetValue(r, Value());
			} else {
				assert(s_chunk.column_count > 0);
				vector.SetValue(r, s_chunk.data[0].GetValue(0));
			}
			break;

		case SubqueryType::IN: // in case is handled separately above
		case SubqueryType::EXISTS:
			vector.SetValue(r, Value::BOOLEAN(s_chunk.size() != 0));
			break;
		default:
			throw NotImplementedException("Subquery type not implemented");
		}
	}
	chunk = old_chunk;
	expr.stats.Verify(vector);
}
