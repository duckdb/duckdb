
#include "common/vector_operations/vector_operations.hpp"
#include "execution/expression_executor.hpp"

#include "parser/expression/operator_expression.hpp"
#include "parser/expression/subquery_expression.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<Expression> ExpressionExecutor::Visit(OperatorExpression &expr) {
	// special handling for special snowflake 'IN'
	// IN has n children
	if (expr.type == ExpressionType::COMPARE_IN ||
	    expr.type == ExpressionType::COMPARE_NOT_IN) {
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
				if (VectorOperations::AnyTrue(comp_res)) {
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
		if (expr.type == ExpressionType::COMPARE_NOT_IN) {
			// invert result
			vector.Initialize(TypeId::BOOLEAN);
			VectorOperations::Not(result, vector);
		} else {
			// just move result
			result.Move(vector);
		}
		expr.stats.Verify(vector);
		return nullptr;
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
	return nullptr;
}