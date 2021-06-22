#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/common/types/chunk_collection.hpp"

namespace duckdb {

void Case(Vector &res_true, Vector &res_false, Vector &result, SelectionVector &tside, idx_t tcount,
          SelectionVector &fside, idx_t fcount);

struct CaseExpressionState : public ExpressionState {
	CaseExpressionState(const Expression &expr, ExpressionExecutorState &root)
	    : ExpressionState(expr, root), true_sel(STANDARD_VECTOR_SIZE), false_sel(STANDARD_VECTOR_SIZE) {
	}

	SelectionVector true_sel;
	SelectionVector false_sel;
};

unique_ptr<ExpressionState> ExpressionExecutor::InitializeState(const BoundCaseExpression &expr,
                                                                ExpressionExecutorState &root) {
	auto result = make_unique<CaseExpressionState>(expr, root);
	result->AddChild(expr.check.get());
	result->AddChild(expr.result_if_true.get());
	result->AddChild(expr.result_if_false.get());
	result->Finalize();
	return result;
}

void ExpressionExecutor::Execute(const BoundCaseExpression &expr, ExpressionState *state_p, const SelectionVector *sel,
                                 idx_t count, Vector &result) {
	auto state = (CaseExpressionState *)state_p;

	state->intermediate_chunk.Reset();
	auto &res_true = state->intermediate_chunk.data[1];
	auto &res_false = state->intermediate_chunk.data[2];

	auto check_state = state->child_states[0].get();
	auto res_true_state = state->child_states[1].get();
	auto res_false_state = state->child_states[2].get();

	// first execute the check expression
	auto &true_sel = state->true_sel;
	auto &false_sel = state->false_sel;
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

template <class T>
void TemplatedFillLoop(Vector &vector, Vector &result, SelectionVector &sel, sel_t count) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto res = FlatVector::GetData<T>(result);
	auto &result_mask = FlatVector::Validity(result);
	if (vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		auto data = ConstantVector::GetData<T>(vector);
		if (ConstantVector::IsNull(vector)) {
			for (idx_t i = 0; i < count; i++) {
				result_mask.SetInvalid(sel.get_index(i));
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
			result_mask.Set(res_idx, vdata.validity.RowIsValid(source_idx));
		}
	}
}

void ValidityFillLoop(Vector &vector, Vector &result, SelectionVector &sel, sel_t count) {
	result.SetVectorType(VectorType::FLAT_VECTOR);
	auto &result_mask = FlatVector::Validity(result);
	if (vector.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(vector)) {
			for (idx_t i = 0; i < count; i++) {
				result_mask.SetInvalid(sel.get_index(i));
			}
		}
	} else {
		VectorData vdata;
		vector.Orrify(count, vdata);
		for (idx_t i = 0; i < count; i++) {
			auto source_idx = vdata.sel->get_index(i);
			auto res_idx = sel.get_index(i);

			result_mask.Set(res_idx, vdata.validity.RowIsValid(source_idx));
		}
	}
}

template <class T>
void TemplatedCaseLoop(Vector &res_true, Vector &res_false, Vector &result, SelectionVector &tside, idx_t tcount,
                       SelectionVector &fside, idx_t fcount) {
	TemplatedFillLoop<T>(res_true, result, tside, tcount);
	TemplatedFillLoop<T>(res_false, result, fside, fcount);
}

void ValidityCaseLoop(Vector &res_true, Vector &res_false, Vector &result, SelectionVector &tside, idx_t tcount,
                      SelectionVector &fside, idx_t fcount) {
	ValidityFillLoop(res_true, result, tside, tcount);
	ValidityFillLoop(res_false, result, fside, fcount);
}

void Case(Vector &res_true, Vector &res_false, Vector &result, SelectionVector &tside, idx_t tcount,
          SelectionVector &fside, idx_t fcount) {
	D_ASSERT(res_true.GetType() == res_false.GetType() && res_true.GetType() == result.GetType());

	switch (result.GetType().InternalType()) {
	case PhysicalType::BOOL:
	case PhysicalType::INT8:
		TemplatedCaseLoop<int8_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::INT16:
		TemplatedCaseLoop<int16_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::INT32:
		TemplatedCaseLoop<int32_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::INT64:
		TemplatedCaseLoop<int64_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::UINT8:
		TemplatedCaseLoop<uint8_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::UINT16:
		TemplatedCaseLoop<uint16_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::UINT32:
		TemplatedCaseLoop<uint32_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::UINT64:
		TemplatedCaseLoop<uint64_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::INT128:
		TemplatedCaseLoop<hugeint_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::FLOAT:
		TemplatedCaseLoop<float>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::DOUBLE:
		TemplatedCaseLoop<double>(res_true, res_false, result, tside, tcount, fside, fcount);
		break;
	case PhysicalType::VARCHAR:
		TemplatedCaseLoop<string_t>(res_true, res_false, result, tside, tcount, fside, fcount);
		StringVector::AddHeapReference(result, res_true);
		StringVector::AddHeapReference(result, res_false);
		break;
	case PhysicalType::STRUCT: {
		auto &res_true_entries = StructVector::GetEntries(res_true);
		auto &res_false_entries = StructVector::GetEntries(res_false);
		auto &result_entries = StructVector::GetEntries(result);
		D_ASSERT(res_true_entries.size() == res_false_entries.size() &&
		         res_true_entries.size() == result_entries.size());
		ValidityCaseLoop(res_true, res_false, result, tside, tcount, fside, fcount);
		for (idx_t i = 0; i < res_true_entries.size(); i++) {
			Case(*res_true_entries[i], *res_false_entries[i], *result_entries[i], tside, tcount, fside, fcount);
		}
		break;
	}
	case PhysicalType::LIST: {
		idx_t offset = 0;

		auto &true_child = ListVector::GetEntry(res_true);
		offset += ListVector::GetListSize(res_true);
		ListVector::Append(result, true_child, ListVector::GetListSize(res_true));

		auto &false_child = ListVector::GetEntry(res_false);
		ListVector::Append(result, false_child, ListVector::GetListSize(res_false));

		// all the false offsets need to be incremented by true_child.count
		TemplatedFillLoop<list_entry_t>(res_true, result, tside, tcount);

		// FIXME the nullmask here is likely borked
		// TODO uuugly
		VectorData fdata;
		res_false.Orrify(fcount, fdata);

		auto data = (list_entry_t *)fdata.data;
		auto res = FlatVector::GetData<list_entry_t>(result);
		auto &mask = FlatVector::Validity(result);

		for (idx_t i = 0; i < fcount; i++) {
			auto fidx = fdata.sel->get_index(i);
			auto res_idx = fside.get_index(i);
			auto list_entry = data[fidx];
			list_entry.offset += offset;
			res[res_idx] = list_entry;
			mask.Set(res_idx, fdata.validity.RowIsValid(fidx));
		}

		result.Verify(tside, tcount);
		result.Verify(fside, fcount);
		break;
	}
	default:
		throw NotImplementedException("Unimplemented type for case expression: %s", result.GetType().ToString());
	}
}

} // namespace duckdb
