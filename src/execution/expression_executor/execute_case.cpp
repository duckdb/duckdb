#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

using namespace duckdb;
using namespace std;

void Case(Vector &res_true, Vector &res_false, Vector &result, SelectionVector &tside, idx_t tcount,
          SelectionVector &fside, idx_t fcount);

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(BoundCaseExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<ExpressionState>(expr, root);
	result->AddChild(expr.check.get());
	result->AddChild(expr.result_if_true.get());
	result->AddChild(expr.result_if_false.get());
	return result;
}

void ExpressionExecutor::Execute(BoundCaseExpression &expr, ExpressionState *state, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	Vector res_true(expr.result_if_true->return_type), res_false(expr.result_if_false->return_type);

	auto check_state = state->child_states[0].get();
	auto res_true_state = state->child_states[1].get();
	auto res_false_state = state->child_states[2].get();

	// first execute the check expression
	SelectionVector true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE);
	idx_t tcount = Select(*expr.check, check_state, sel, count, &true_sel, &false_sel);
	idx_t fcount = count - tcount;
	if (fcount == 0) {
		// everything is true, only execute TRUE side
		Execute(*expr.result_if_true, res_true_state, sel, count, result);
	} else if (tcount == 0) {
		// everything is false, only execute FALSE side
		Execute(*expr.result_if_false, res_false_state, sel, count, result);
	} else {
		// have to execute both and mix and match
		Execute(*expr.result_if_true, res_true_state, &true_sel, tcount, res_true);
		Execute(*expr.result_if_false, res_false_state, &false_sel, fcount, res_false);

		Case(res_true, res_false, result, true_sel, tcount, false_sel, fcount);
		if (sel) {
			result.Slice(*sel, count);
		}
	}
}

template <class T> void fill_loop(Vector &vector, Vector &result, SelectionVector &sel, sel_t count) {
	auto res = FlatVector::GetData<T>(result);
	auto &result_nullmask = FlatVector::Nullmask(result);
	if (vector.vector_type == VectorType::CONSTANT_VECTOR) {
		auto data = ConstantVector::GetData<T>(vector);
		if (ConstantVector::IsNull(vector)) {
			for (idx_t i = 0; i < count; i++) {
				result_nullmask[sel.get_index(i)] = true;
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				res[sel.get_index(i)] = *data;
			}
		}
	} else {
		VectorData vdata;
		vector.Orrify(count, vdata);
		auto data = (T *)vdata.data;
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = vdata.sel->get_index(i);
			auto res_idx = sel.get_index(i);

			res[res_idx] = data[source_idx];
			result_nullmask[res_idx] = (*vdata.nullmask)[source_idx];
		}
	}
}

template <class T>
void case_loop(Vector &res_true, Vector &res_false, Vector &result, SelectionVector &tside, idx_t tcount,
               SelectionVector &fside, idx_t fcount) {
	fill_loop<T>(res_true, result, tside, tcount);
	fill_loop<T>(res_false, result, fside, fcount);
}

void Case(Vector &res_true, Vector &res_false, Vector &result, SelectionVector &tside, idx_t tcount,
          SelectionVector &fside, idx_t fcount) {
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
		StringVector::AddHeapReference(result, res_true);
		StringVector::AddHeapReference(result, res_false);
		break;
	case TypeId::LIST: {
		auto result_cc = make_unique<ChunkCollection>();
		ListVector::SetEntry(result, move(result_cc));

		auto &result_child = ListVector::GetEntry(result);
		idx_t offset = 0;
		if (ListVector::HasEntry(res_true)) {
			auto &true_child = ListVector::GetEntry(res_true);
			assert(true_child.types.size() == 1);
			offset += true_child.count;
			result_child.Append(true_child);
		}
		if (ListVector::HasEntry(res_false)) {
			auto &false_child = ListVector::GetEntry(res_false);
			assert(false_child.types.size() == 1);
			result_child.Append(false_child);
		}

		// all the false offsets need to be incremented by true_child.count
		fill_loop<list_entry_t>(res_true, result, tside, tcount);

		// FIXME the nullmask here is likely borked
		// TODO uuugly
		VectorData fdata;
		res_false.Orrify(fcount, fdata);

		auto data = (list_entry_t *)fdata.data;
		auto res = FlatVector::GetData<list_entry_t>(result);
		auto &mask = FlatVector::Nullmask(result);

		for (idx_t i = 0; i < fcount; i++) {
			auto fidx = fdata.sel->get_index(i);
			auto res_idx = fside.get_index(i);
			auto list_entry = data[fidx];
			list_entry.offset += offset;
			res[res_idx] = list_entry;
			mask[res_idx] = (*fdata.nullmask)[fidx];
		}

		result.Verify(tcount + fcount);
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for case expression: %s",
		                              TypeIdToString(result.type).c_str());
	}
}
