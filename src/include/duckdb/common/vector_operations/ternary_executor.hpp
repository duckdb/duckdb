//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/vector_operations/ternary_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

#include <functional>

namespace duckdb {

struct TernaryExecutor {
private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE, class FUN>
	static inline void ExecuteLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
	                               RESULT_TYPE *__restrict result_data, idx_t count, const SelectionVector &asel,
	                               const SelectionVector &bsel, const SelectionVector &csel, ValidityMask &avalidity,
	                               ValidityMask &bvalidity, ValidityMask &cvalidity, ValidityMask &result_validity,
	                               FUN fun) {
		if (!avalidity.AllValid() || !bvalidity.AllValid() || !cvalidity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto cidx = csel.get_index(i);
				if (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx) && cvalidity.RowIsValid(cidx)) {
					result_data[i] = fun(adata[aidx], bdata[bidx], cdata[cidx]);
				} else {
					result_validity.SetInvalid(i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto cidx = csel.get_index(i);
				result_data[i] = fun(adata[aidx], bdata[bidx], cdata[cidx]);
			}
		}
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class RESULT_TYPE,
	          class FUN = std::function<RESULT_TYPE(A_TYPE, B_TYPE, C_TYPE)>>
	static void Execute(Vector &a, Vector &b, Vector &c, Vector &result, idx_t count, FUN fun) {
		if (a.GetVectorType() == VectorType::CONSTANT_VECTOR && b.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    c.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			if (ConstantVector::IsNull(a) || ConstantVector::IsNull(b) || ConstantVector::IsNull(c)) {
				ConstantVector::SetNull(result, true);
			} else {
				auto adata = ConstantVector::GetData<A_TYPE>(a);
				auto bdata = ConstantVector::GetData<B_TYPE>(b);
				auto cdata = ConstantVector::GetData<C_TYPE>(c);
				auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
				result_data[0] = fun(*adata, *bdata, *cdata);
			}
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);

			VectorData adata, bdata, cdata;
			a.Orrify(count, adata);
			b.Orrify(count, bdata);
			c.Orrify(count, cdata);

			ExecuteLoop<A_TYPE, B_TYPE, C_TYPE, RESULT_TYPE>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data,
			    FlatVector::GetData<RESULT_TYPE>(result), count, *adata.sel, *bdata.sel, *cdata.sel, adata.validity,
			    bdata.validity, cdata.validity, FlatVector::Validity(result), fun);
		}
	}

private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
	static inline idx_t SelectLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
	                               const SelectionVector *result_sel, idx_t count, const SelectionVector &asel,
	                               const SelectionVector &bsel, const SelectionVector &csel, ValidityMask &avalidity,
	                               ValidityMask &bvalidity, ValidityMask &cvalidity, SelectionVector *true_sel,
	                               SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = result_sel->get_index(i);
			auto aidx = asel.get_index(i);
			auto bidx = bsel.get_index(i);
			auto cidx = csel.get_index(i);
			bool comparison_result =
			    (NO_NULL || (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx) && cvalidity.RowIsValid(cidx))) &&
			    OP::Operation(adata[aidx], bdata[bidx], cdata[cidx]);
			if (HAS_TRUE_SEL) {
				true_sel->set_index(true_count, result_idx);
				true_count += comparison_result;
			}
			if (HAS_FALSE_SEL) {
				false_sel->set_index(false_count, result_idx);
				false_count += !comparison_result;
			}
		}
		if (HAS_TRUE_SEL) {
			return true_count;
		} else {
			return count - false_count;
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP, bool NO_NULL>
	static inline idx_t SelectLoopSelSwitch(VectorData &adata, VectorData &bdata, VectorData &cdata,
	                                        const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                                        SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, NO_NULL, true, true>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, count, *adata.sel, *bdata.sel,
			    *cdata.sel, adata.validity, bdata.validity, cdata.validity, true_sel, false_sel);
		} else if (true_sel) {
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, NO_NULL, true, false>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, count, *adata.sel, *bdata.sel,
			    *cdata.sel, adata.validity, bdata.validity, cdata.validity, true_sel, false_sel);
		} else {
			D_ASSERT(false_sel);
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, OP, NO_NULL, false, true>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, count, *adata.sel, *bdata.sel,
			    *cdata.sel, adata.validity, bdata.validity, cdata.validity, true_sel, false_sel);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
	static inline idx_t SelectLoopSwitch(VectorData &adata, VectorData &bdata, VectorData &cdata,
	                                     const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                                     SelectionVector *false_sel) {
		if (!adata.validity.AllValid() || !bdata.validity.AllValid() || !cdata.validity.AllValid()) {
			return SelectLoopSelSwitch<A_TYPE, B_TYPE, C_TYPE, OP, false>(adata, bdata, cdata, sel, count, true_sel,
			                                                              false_sel);
		} else {
			return SelectLoopSelSwitch<A_TYPE, B_TYPE, C_TYPE, OP, true>(adata, bdata, cdata, sel, count, true_sel,
			                                                             false_sel);
		}
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class OP>
	static idx_t Select(Vector &a, Vector &b, Vector &c, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		SelectionVector owned_sel;
		if (!sel) {
			sel = FlatVector::IncrementalSelectionVector(count, owned_sel);
		}
		VectorData adata, bdata, cdata;
		a.Orrify(count, adata);
		b.Orrify(count, bdata);
		c.Orrify(count, cdata);

		return SelectLoopSwitch<A_TYPE, B_TYPE, C_TYPE, OP>(adata, bdata, cdata, sel, count, true_sel, false_sel);
	}
};

} // namespace duckdb
