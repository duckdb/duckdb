#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

using namespace duckdb;
using namespace std;

void Case(Vector &res_true, Vector &res_false, Vector &result, sel_t tside[], idx_t tcount, sel_t fside[],
          idx_t fcount);

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundCaseExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.check.get());
	result->AddChild(expr.result_if_true.get());
	result->AddChild(expr.result_if_false.get());
	return result;
}

void ExpressionExecutor::Execute(BoundCaseExpression &expr, ExpressionState *state, Vector &result) {
	Vector check(GetCardinality(), expr.check->return_type);
	Vector res_true(GetCardinality(), expr.result_if_true->return_type);
	Vector res_false(GetCardinality(), expr.result_if_false->return_type);

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
		idx_t tcount = 0, fcount = 0;
		VectorOperations::Exec(check, [&](idx_t i, idx_t k) {
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

			Case(res_true, res_false, result, tside, tcount, fside, fcount);
		}
	}
}

template <class T> void fill_loop(Vector &vector, Vector &result, sel_t sel[], sel_t count) {
	auto data = (T *)vector.GetData();
	auto res = (T *)result.GetData();
	if (vector.vector_type == VectorType::CONSTANT_VECTOR) {
		if (vector.nullmask[0]) {
			for (idx_t i = 0; i < count; i++) {
				result.nullmask[sel[i]] = true;
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				res[sel[i]] = data[0];
			}
		}
	} else {
		for (idx_t i = 0; i < count; i++) {
			res[sel[i]] = data[sel[i]];
			result.nullmask[sel[i]] = vector.nullmask[sel[i]];
		}
	}
}

template <class T>
void case_loop(Vector &res_true, Vector &res_false, Vector &result, sel_t tside[], idx_t tcount, sel_t fside[],
               idx_t fcount) {
	fill_loop<T>(res_true, result, tside, tcount);
	fill_loop<T>(res_false, result, fside, fcount);
}

void Case(Vector &res_true, Vector &res_false, Vector &result, sel_t tside[], idx_t tcount, sel_t fside[],
          idx_t fcount) {
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
		case_loop<string_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		result.AddHeapReference(res_true);
		result.AddHeapReference(res_false);
		break;
	case TypeId::LIST:
	{
		auto& true_child = res_true.GetListEntry();
		auto& false_child = res_false.GetListEntry();

		assert(true_child.types.size() == 1);
		assert(false_child.types.size() == 1);
		if(true_child.types[0] != false_child.types[0]) {
			throw TypeMismatchException(true_child.types[0], false_child.types[0], "CASE on LISTs requires matching list content types");
		}

		vector<TypeId> child_type;
		child_type.push_back(true_child.types[0]);

		DataChunk true_append;
		DataChunk false_append;

		true_append.InitializeEmpty(child_type);
		false_append.InitializeEmpty(child_type);
		true_append.SetCardinality(tcount, tside);
		false_append.SetCardinality(fcount, fside);
		true_append.data[0].Reference(res_true);
		false_append.data[0].Reference(res_false);

		true_append.Verify();
		false_append.Verify();

		auto result_cc = make_unique<ChunkCollection>();
		result.SetListEntry(move(result_cc));

		auto& result_child = result.GetListEntry();

		result_child.Append(true_child);
		result_child.Append(false_child);

		// all the false offsets need to be incremented by true_child.count
		fill_loop<list_entry_t>(res_true, result, tside, tcount);

		// FIXME the nullmask here is likely borked
		// TODO uuugly
		auto data = (list_entry_t *)res_false.GetData();
		auto res = (list_entry_t *)result.GetData();
		if (res_false.vector_type == VectorType::CONSTANT_VECTOR) {
			if (res_false.nullmask[0]) {
				for (idx_t i = 0; i < fcount; i++) {
					result.nullmask[fside[i]] = true;
				}
			} else {
				for (idx_t i = 0; i < fcount; i++) {
					auto list_entry = data[0];
					list_entry.offset += true_child.count;
					res[fside[i]] = list_entry;
				}
			}
		} else {
			for (idx_t i = 0; i < fcount; i++) {
				auto list_entry = data[fside[i]];
				list_entry.offset += true_child.count;
				res[fside[i]] = list_entry;
				result.nullmask[fside[i]] = res_false.nullmask[fside[i]];
			}
		}

		result.Verify();

		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for case expression: %s", TypeIdToString(result.type).c_str());
	}
}
