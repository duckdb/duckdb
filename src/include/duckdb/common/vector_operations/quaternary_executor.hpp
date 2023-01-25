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

struct QuaternaryLambdaWrapper {
	template <class FUN, class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUN fun, A_TYPE a, B_TYPE b, C_TYPE c, D_TYPE d, ValidityMask &mask,
	                                    idx_t idx) {
		return fun(a, b, c, d);
	}
};

struct QuaternaryLambdaWrapperWithNulls {
	template <class FUN, class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class RESULT_TYPE>
	static inline RESULT_TYPE Operation(FUN fun, A_TYPE a, B_TYPE b, C_TYPE c, D_TYPE d, ValidityMask &mask,
	                                    idx_t idx) {
		return fun(a, b, c, d, mask, idx);
	}
};

struct QuaternaryExecutor {
private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class RESULT_TYPE, class OPWRAPPER, class FUN>
	static inline void ExecuteLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
	                               D_TYPE *__restrict ddata, RESULT_TYPE *__restrict result_data, idx_t count,
	                               const SelectionVector &asel, const SelectionVector &bsel,
	                               const SelectionVector &csel, const SelectionVector &dsel, ValidityMask &avalidity,
	                               ValidityMask &bvalidity, ValidityMask &cvalidity, ValidityMask &dvalidity,
	                               ValidityMask &result_validity, FUN fun) {
		if (!avalidity.AllValid() || !bvalidity.AllValid() || !cvalidity.AllValid() || !dvalidity.AllValid()) {
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto cidx = csel.get_index(i);
				auto didx = dsel.get_index(i);
				if (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx) && cvalidity.RowIsValid(cidx) &&
				    dvalidity.RowIsValid(didx)) {
					result_data[i] = OPWRAPPER::template Operation<FUN, A_TYPE, B_TYPE, C_TYPE, D_TYPE, RESULT_TYPE>(
					    fun, adata[aidx], bdata[bidx], cdata[cidx], ddata[didx], result_validity, i);
				} else {
					result_validity.SetInvalid(i);
				}
			}
		} else {
			for (idx_t i = 0; i < count; i++) {
				auto aidx = asel.get_index(i);
				auto bidx = bsel.get_index(i);
				auto cidx = csel.get_index(i);
				auto didx = dsel.get_index(i);
				result_data[i] = OPWRAPPER::template Operation<FUN, A_TYPE, B_TYPE, C_TYPE, D_TYPE, RESULT_TYPE>(
				    fun, adata[aidx], bdata[bidx], cdata[cidx], ddata[didx], result_validity, i);
			}
		}
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class RESULT_TYPE, class OPWRAPPER, class FUN>
	static void ExecuteGeneric(Vector &a, Vector &b, Vector &c, Vector &d, Vector &result, idx_t count, FUN fun) {
		if (a.GetVectorType() == VectorType::CONSTANT_VECTOR && b.GetVectorType() == VectorType::CONSTANT_VECTOR &&
		    c.GetVectorType() == VectorType::CONSTANT_VECTOR && d.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			if (ConstantVector::IsNull(a) || ConstantVector::IsNull(b) || ConstantVector::IsNull(c) ||
			    ConstantVector::IsNull(d)) {
				ConstantVector::SetNull(result, true);
			} else {
				auto adata = ConstantVector::GetData<A_TYPE>(a);
				auto bdata = ConstantVector::GetData<B_TYPE>(b);
				auto cdata = ConstantVector::GetData<C_TYPE>(c);
				auto ddata = ConstantVector::GetData<D_TYPE>(d);
				auto result_data = ConstantVector::GetData<RESULT_TYPE>(result);
				auto &result_validity = ConstantVector::Validity(result);
				result_data[0] = OPWRAPPER::template Operation<FUN, A_TYPE, B_TYPE, C_TYPE, D_TYPE, RESULT_TYPE>(
				    fun, adata[0], bdata[0], cdata[0], ddata[0], result_validity, 0);
			}
		} else {
			result.SetVectorType(VectorType::FLAT_VECTOR);

			UnifiedVectorFormat adata, bdata, cdata, ddata;
			a.ToUnifiedFormat(count, adata);
			b.ToUnifiedFormat(count, bdata);
			c.ToUnifiedFormat(count, cdata);
			d.ToUnifiedFormat(count, ddata);

			ExecuteLoop<A_TYPE, B_TYPE, C_TYPE, D_TYPE, RESULT_TYPE, OPWRAPPER>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, (D_TYPE *)ddata.data,
			    FlatVector::GetData<RESULT_TYPE>(result), count, *adata.sel, *bdata.sel, *cdata.sel, *ddata.sel,
			    adata.validity, bdata.validity, cdata.validity, ddata.validity, FlatVector::Validity(result), fun);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class RESULT_TYPE,
	          class FUN = std::function<RESULT_TYPE(A_TYPE, B_TYPE, C_TYPE, D_TYPE)>>
	static void Execute(Vector &a, Vector &b, Vector &c, Vector &d, Vector &result, idx_t count, FUN fun) {
		ExecuteGeneric<A_TYPE, B_TYPE, C_TYPE, D_TYPE, RESULT_TYPE, QuaternaryLambdaWrapper, FUN>(a, b, c, d, result,
		                                                                                          count, fun);
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class RESULT_TYPE,
	          class FUN = std::function<RESULT_TYPE(A_TYPE, B_TYPE, C_TYPE, D_TYPE, ValidityMask &, idx_t)>>
	static void ExecuteWithNulls(Vector &a, Vector &b, Vector &c, Vector &d, Vector &result, idx_t count, FUN fun) {
		ExecuteGeneric<A_TYPE, B_TYPE, C_TYPE, D_TYPE, RESULT_TYPE, QuaternaryLambdaWrapperWithNulls, FUN>(
		    a, b, c, d, result, count, fun);
	}

private:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class OP, bool NO_NULL, bool HAS_TRUE_SEL,
	          bool HAS_FALSE_SEL>
	static inline idx_t SelectLoop(A_TYPE *__restrict adata, B_TYPE *__restrict bdata, C_TYPE *__restrict cdata,
	                               D_TYPE *__restrict ddata, const SelectionVector *result_sel, idx_t count,
	                               const SelectionVector &asel, const SelectionVector &bsel,
	                               const SelectionVector &csel, const SelectionVector &dsel, ValidityMask &avalidity,
	                               ValidityMask &bvalidity, ValidityMask &cvalidity, ValidityMask &dvalidity,
	                               SelectionVector *true_sel, SelectionVector *false_sel) {
		idx_t true_count = 0, false_count = 0;
		for (idx_t i = 0; i < count; i++) {
			auto result_idx = result_sel->get_index(i);
			auto aidx = asel.get_index(i);
			auto bidx = bsel.get_index(i);
			auto cidx = csel.get_index(i);
			auto didx = dsel.get_index(i);
			bool comparison_result = (NO_NULL || (avalidity.RowIsValid(aidx) && bvalidity.RowIsValid(bidx) &&
			                                      cvalidity.RowIsValid(cidx) && dvalidity.RowIsValid(didx))) &&
			                         OP::Operation(adata[aidx], bdata[bidx], cdata[cidx], ddata[didx]);
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

	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class OP, bool NO_NULL>
	static inline idx_t SelectLoopSelSwitch(UnifiedVectorFormat &adata, UnifiedVectorFormat &bdata,
	                                        UnifiedVectorFormat &cdata, UnifiedVectorFormat &ddata,
	                                        const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                                        SelectionVector *false_sel) {
		if (true_sel && false_sel) {
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, D_TYPE, OP, NO_NULL, true, true>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, (D_TYPE *)ddata.data, sel, count,
			    *adata.sel, *bdata.sel, *cdata.sel, *ddata.sel, adata.validity, bdata.validity, cdata.validity,
			    ddata.validity, true_sel, false_sel);
		} else if (true_sel) {
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, D_TYPE, OP, NO_NULL, true, false>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, (D_TYPE *)ddata.data, sel, count,
			    *adata.sel, *bdata.sel, *cdata.sel, *ddata.sel, adata.validity, bdata.validity, cdata.validity,
			    ddata.validity, true_sel, false_sel);
		} else {
			D_ASSERT(false_sel);
			return SelectLoop<A_TYPE, B_TYPE, C_TYPE, D_TYPE, OP, NO_NULL, false, true>(
			    (A_TYPE *)adata.data, (B_TYPE *)bdata.data, (C_TYPE *)cdata.data, sel, (D_TYPE *)ddata.data, count,
			    *adata.sel, *bdata.sel, *cdata.sel, adata.validity, bdata.validity, cdata.validity, ddata.validity,
			    true_sel, false_sel);
		}
	}

	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class OP>
	static inline idx_t SelectLoopSwitch(UnifiedVectorFormat &adata, UnifiedVectorFormat &bdata,
	                                     UnifiedVectorFormat &cdata, UnifiedVectorFormat &ddata,
	                                     const SelectionVector *sel, idx_t count, SelectionVector *true_sel,
	                                     SelectionVector *false_sel) {
		if (!adata.validity.AllValid() || !bdata.validity.AllValid() || !cdata.validity.AllValid() ||
		    !ddata.validity.AllValid()) {
			return SelectLoopSelSwitch<A_TYPE, B_TYPE, C_TYPE, D_TYPE, OP, false>(adata, bdata, cdata, ddata, sel,
			                                                                      count, true_sel, false_sel);
		} else {
			return SelectLoopSelSwitch<A_TYPE, B_TYPE, C_TYPE, D_TYPE, OP, true>(adata, bdata, cdata, ddata, sel, count,
			                                                                     true_sel, false_sel);
		}
	}

public:
	template <class A_TYPE, class B_TYPE, class C_TYPE, class D_TYPE, class OP>
	static idx_t Select(Vector &a, Vector &b, Vector &c, Vector &d, const SelectionVector *sel, idx_t count,
	                    SelectionVector *true_sel, SelectionVector *false_sel) {
		if (!sel) {
			sel = FlatVector::IncrementalSelectionVector();
		}
		UnifiedVectorFormat adata, bdata, cdata, ddata;
		a.ToUnifiedFormat(count, adata);
		b.ToUnifiedFormat(count, bdata);
		c.ToUnifiedFormat(count, cdata);
		d.ToUnifiedFormat(count, ddata);

		return SelectLoopSwitch<A_TYPE, B_TYPE, C_TYPE, D_TYPE, OP>(adata, bdata, cdata, ddata, sel, count, true_sel,
		                                                            false_sel);
	}
};

} // namespace duckdb
