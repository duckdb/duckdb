#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"

using namespace duckdb;
using namespace std;

void Case(Vector &res_true, Vector &res_false, Vector &result, sel_t tside[], index_t tcount, sel_t fside[],
          index_t fcount);

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundCaseExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.check.get());
	result->AddChild(expr.result_if_true.get());
	result->AddChild(expr.result_if_false.get());
	return result;
}

void ExpressionExecutor::Execute(BoundCaseExpression &expr, ExpressionState *state, Vector &result) {
	Vector check(expr.check->return_type);
	Vector res_true(expr.result_if_true->return_type);
	Vector res_false(expr.result_if_false->return_type);

	auto check_state = state->child_states[0].get();
	auto res_true_state = state->child_states[1].get();
	auto res_false_state = state->child_states[2].get();

	// first execute the check expression
	Execute(*expr.check, check_state, check);
	auto check_data = (bool *)check.GetData();
	if (check.vector_type == VectorType::CONSTANT_VECTOR) {
		// constant check: only need to execute one side
		if (!check_data[0] || check.nullmask[0]) {
			// constant false or NULL; result is FALSE
			Execute(*expr.result_if_false, res_false_state, result);
		} else {
			// constant true; result is TRUE
			Execute(*expr.result_if_true, res_true_state, result);
		}
	} else {
		// check is not a constant
		// first set up the sel vectors for both sides
		sel_t tside[STANDARD_VECTOR_SIZE], fside[STANDARD_VECTOR_SIZE];
		index_t tcount = 0, fcount = 0;
		VectorOperations::Exec(check, [&](index_t i, index_t k) {
			if (!check_data[i] || check.nullmask[i]) {
				fside[fcount++] = i;
			} else {
				tside[tcount++] = i;
			}
		});
		if (fcount == 0) {
			// everything is true, only execute TRUE side
			Execute(*expr.result_if_true, res_true_state, result);
		} else if (tcount == 0) {
			// everything is false, only execute FALSE side
			Execute(*expr.result_if_false, res_false_state, result);
		} else {
			// have to execute both and mix and match
			Execute(*expr.result_if_true, res_true_state, res_true);
			Execute(*expr.result_if_false, res_false_state, res_false);

			result.SetSelVector(check.sel_vector());
			result.SetCount(check.size());

			Case(res_true, res_false, result, tside, tcount, fside, fcount);
		}
	}
}

template <class T> void fill_loop(Vector &vector, Vector &result, sel_t sel[], sel_t count) {
	auto data = (T *)vector.GetData();
	auto res = (T *)result.GetData();
	if (vector.vector_type == VectorType::CONSTANT_VECTOR) {
		if (vector.nullmask[0]) {
			for (index_t i = 0; i < count; i++) {
				result.nullmask[sel[i]] = true;
			}
		} else {
			for (index_t i = 0; i < count; i++) {
				res[sel[i]] = data[0];
			}
		}
	} else {
		for (index_t i = 0; i < count; i++) {
			res[sel[i]] = data[sel[i]];
			result.nullmask[sel[i]] = vector.nullmask[sel[i]];
		}
	}
}

template <class T>
void case_loop(Vector &res_true, Vector &res_false, Vector &result, sel_t tside[], index_t tcount, sel_t fside[],
               index_t fcount) {
	fill_loop<T>(res_true, result, tside, tcount);
	fill_loop<T>(res_false, result, fside, fcount);
}

void Case(Vector &res_true, Vector &res_false, Vector &result, sel_t tside[], index_t tcount, sel_t fside[],
          index_t fcount) {
	assert(res_true.type == res_false.type && res_true.type == result.type);

	switch (result.type) {
	case TypeId::BOOL:
	case TypeId::INT8:
		case_loop<int8_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case TypeId::INT16:
		case_loop<int16_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case TypeId::INT32:
		case_loop<int32_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case TypeId::INT64:
		case_loop<int64_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case TypeId::FLOAT:
		case_loop<float>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case TypeId::DOUBLE:
		case_loop<double>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case TypeId::VARCHAR:
		case_loop<const char *>(res_true, res_false, result, tside, tcount, fside, fcount);
		result.AddHeapReference(res_true);
		result.AddHeapReference(res_false);
		break;
	default:
		throw NotImplementedException("Unimplemented type for case expression");
	}
}
