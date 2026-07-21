//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor/bitmap_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/bitmap_selection_vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_result.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector/for_view.hpp"
#include "duckdb/common/vector_operations/comparison_bitmap.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

inline bool TryGetBitmapComparisonInfo(const Expression &expr, BitmapComparisonInfo &info) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	const auto raw_op = expr.GetExpressionType();
	if (raw_op == ExpressionType::COMPARE_DISTINCT_FROM) {
		return false;
	}
	// NOT DISTINCT FROM equals `=` only when a NULL can't meet a NULL. That holds against a (non-null) constant, but
	// NOT for column-vs-column: two NULLs are "not distinct" (true) yet `=` AND-s out validity (false). So the col-col
	// path must reject it - decorrelation matches correlated keys this way, and mapping it to `=` drops NULL matches.
	const auto op = raw_op == ExpressionType::COMPARE_NOT_DISTINCT_FROM ? ExpressionType::COMPARE_EQUAL : raw_op;
	auto &comparison = expr.Cast<BoundFunctionExpression>();
	auto &left = BoundComparisonExpression::Left(comparison);
	auto &right = BoundComparisonExpression::Right(comparison);

	if (left.GetExpressionClass() == ExpressionClass::BOUND_REF &&
	    right.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
		info.ref = &left.Cast<BoundReferenceExpression>();
		info.constant = &right.Cast<BoundConstantExpression>();
		info.op = op;
	} else if (left.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT &&
	           right.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		info.ref = &right.Cast<BoundReferenceExpression>();
		info.constant = &left.Cast<BoundConstantExpression>();
		info.op = FlipComparisonExpression(op);
	} else if (left.GetExpressionClass() == ExpressionClass::BOUND_REF &&
	           right.GetExpressionClass() == ExpressionClass::BOUND_REF) {
		if (raw_op == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
			return false;
		}
		info.ref = &left.Cast<BoundReferenceExpression>();
		info.ref2 = &right.Cast<BoundReferenceExpression>();
		info.op = op;
	} else {
		return false;
	}
	return BitmapCmpOpSupported(info.op);
}

inline bool IsBitmapComparisonCandidate(const Expression &expr) {
	if (expr.IsVolatile() || expr.CanThrow()) {
		return false;
	}
	BitmapComparisonInfo info;
	if (!TryGetBitmapComparisonInfo(expr, info)) {
		return false;
	}
	const auto pt = info.ref->GetReturnType().InternalType();
	if (!BitmapCmpTypeSupported(pt)) {
		return false;
	}
	if (info.ref2) {
		// both sides of a bound comparison share the same type
		return true;
	}
	const auto &value = info.constant->GetValue();
	return !value.IsNull() && value.type().InternalType() == pt;
}

inline bool HasBitmapComparisonChild(const BoundConjunctionExpression &expr) {
	for (auto &child : expr.GetChildren()) {
		if (IsBitmapComparisonCandidate(*child)) {
			return true;
		}
	}
	return false;
}

//! A comparison candidate, or an AND of them (e.g. BETWEEN): the whole Select can produce a bitmap.
inline bool IsBitmapSelectCandidate(const Expression &expr) {
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND) {
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			if (!IsBitmapSelectCandidate(*child)) {
				return false;
			}
		}
		return true;
	}
	return IsBitmapComparisonCandidate(expr);
}

struct NarrowCol {
	const_data_ptr_t data = nullptr;
	PhysicalType type = PhysicalType::INVALID;
	const validity_t *validity = nullptr;
};

inline bool ResolveNarrowCol(const Vector &v, NarrowCol &out) {
	const auto vt = v.GetVectorType();
	if (vt == VectorType::FLAT_VECTOR) {
		out.data = FlatVector::GetData(v);
		out.type = v.GetType().InternalType();
		const auto &val = FlatVector::Validity(v);
		out.validity = val.CanHaveNull() ? val.GetData() : nullptr;
		return true;
	}
	if (vt == VectorType::FOR_VECTOR) {
		out.data = FORVector::GetData(v);
		out.type = FORVector::GetStoredType(v);
		auto &val = FORVector::Validity(v);
		out.validity = val.CanHaveNull() ? val.GetData() : nullptr;
		return true;
	}
	return false;
}

inline const Vector *TryGetDictChild(const Vector &v, const SelectionVector *&sel) {
	if (v.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return nullptr;
	}
	return FORVector::TryGetFOR(v, sel);
}

inline void FillConstView(const ForView &view, ExpressionType op, idx_t span, validity_t *t_bm) {
	const validity_t *validity = view.original_validity ? view.original_validity->GetData() : nullptr;
	if (view.always_false || view.always_true) {
		WriteConstantBitmap(view.always_true, span, validity, t_bm);
		return;
	}
	DispatchBitmapType(view.narrow_type, span, [&](auto tag) {
		using T = decltype(tag);
		const auto data = reinterpret_cast<const T *>(view.data);
		const auto constant = static_cast<T>(view.rewritten_constant);
		DispatchBitmapCmpOp<T>(
		    op, [&](auto cmp) { NarrowCmpToBitmap<T, decltype(cmp)>(data, constant, span, validity, t_bm); });
	});
}

inline void FillNarrowCol(const NarrowCol &l, const NarrowCol &r, ExpressionType op, idx_t span, validity_t *t_bm) {
	DispatchBitmapType(l.type, span, [&](auto tag) {
		using T = decltype(tag);
		DispatchBitmapCmpOp<T>(op, [&](auto cmp) {
			NarrowColCmpToBitmap<T, decltype(cmp)>(reinterpret_cast<const T *>(l.data),
			                                       reinterpret_cast<const T *>(r.data), span, l.validity, r.validity,
			                                       t_bm);
		});
	});
}

inline idx_t EmitBitmapSelection(SelectionResult &t, validity_t *t_bm, idx_t len, const SelectionVector *sel,
                                 idx_t count, SelectionResult *bitmap_sel, SelectionVector *true_sel,
                                 SelectionVector *false_sel, SelectionResult &tmp_sel2, SelectionResult &tmp_sel3) {
	validity_t *f_bm = nullptr;
	if (false_sel && !bitmap_sel) {
		f_bm = tmp_sel3.Complement(t, len);
	}
	idx_t result;
	if (sel && sel->IsSet()) {
		tmp_sel2.Initialize(*sel);
		tmp_sel2.ToBitmap(count, len);
		result = t.Intersect(tmp_sel2, len, count, len);
		if (f_bm) {
			tmp_sel3.Intersect(tmp_sel2, len, count, len);
		}
	} else {
		result = BitmapPopcount(t_bm, len);
	}
	if (f_bm) {
		BitmapToSelectionVector(f_bm, len, *false_sel);
	}
	if (!bitmap_sel && true_sel) {
		BitmapToSelectionVector(t_bm, len, *true_sel);
	}
	return result;
}

//! General comparison selection fast path. Evaluates the comparison DENSELY over the input into a
//! bitmap (branchless/autovec), then combines lazily: any input selection is AND-ed in as a bitmap, and the result
//! is emitted to whatever the caller wants (a result bitmap, or a true and/or false selection vector). Returns false
//! (nothing written) for shapes it does not handle, so the caller falls through to generic selection.
inline bool SelectComparisonFromChunk(const BitmapComparisonInfo &info, DataChunk &chunk, const SelectionVector *sel,
                                      idx_t count, SelectionResult *bitmap_sel, SelectionVector *true_sel,
                                      SelectionVector *false_sel, SelectionResult &tmp_sel1, SelectionResult &tmp_sel2,
                                      SelectionResult &tmp_sel3, idx_t &result) {
	// when a bitmap output is requested, true_sel aliases its flat view (see ExpressionExecutor::Select): the
	// comparison result lands in bitmap_sel and true_sel/false_sel are not materialized separately
	auto &col = chunk.data[info.ref->Index()];
	const auto pt = col.GetType().InternalType();
	if (!BitmapCmpTypeSupported(pt)) {
		return false;
	}

	{
		const SelectionVector *ldsel = nullptr;
		auto lchild = TryGetDictChild(col, ldsel);
		if (lchild) {
			const idx_t child_len = lchild->size();
			const bool have_sel = sel && sel->IsSet();
			const idx_t logical_span = have_sel ? chunk.size() : count;
			if (child_len > STANDARD_VECTOR_SIZE || logical_span > STANDARD_VECTOR_SIZE) {
				return false;
			}
			// Compare densely over the child, then project child-space result bits through the dictionary.
			static constexpr idx_t NWORDS = (STANDARD_VECTOR_SIZE + 63) / 64;
			validity_t child_bm[NWORDS];
			NarrowCol l, r;
			ForView view;
			if (info.ref2) {
				const SelectionVector *rdsel = nullptr;
				auto rchild = TryGetDictChild(chunk.data[info.ref2->Index()], rdsel);
				if (!rchild || !rdsel || !SelectionVector::SameSelection(*ldsel, *rdsel) ||
				    lchild->GetVectorType() != rchild->GetVectorType() || !ResolveNarrowCol(*lchild, l) ||
				    !ResolveNarrowCol(*rchild, r) || l.type != r.type) {
					return false;
				}
				FillNarrowCol(l, r, info.op, child_len, child_bm);
			} else {
				const auto &constant = info.constant->GetValue();
				if (constant.IsNull() || constant.type().InternalType() != pt ||
				    !TryResolveForView(*lchild, info.op, constant, view)) {
					return false;
				}
				FillConstView(view, info.op, child_len, child_bm);
			}
			SelectionResult &t = bitmap_sel ? *bitmap_sel : tmp_sel1;
			auto t_bm = reinterpret_cast<validity_t *>(t.PrepareBitmap(logical_span));
			for (idx_t w = 0; w < (logical_span + 63) / 64; w++) {
				const idx_t base = w * 64;
				const idx_t n = MinValue<idx_t>(64, logical_span - base);
				validity_t acc = 0;
				for (idx_t j = 0; j < n; j++) {
					const auto c = ldsel->get_index(base + j);
					acc |= validity_t((child_bm[c >> 6] >> (c & 63)) & 1) << j;
				}
				t_bm[w] = acc;
			}
			result = EmitBitmapSelection(t, t_bm, logical_span, sel, count, bitmap_sel, true_sel, false_sel, tmp_sel2,
			                             tmp_sel3);
			return true;
		}
	}

	NarrowCol l;
	if (!ResolveNarrowCol(col, l)) {
		return false;
	}
	const bool l_for = col.GetVectorType() == VectorType::FOR_VECTOR;

	NarrowCol r;
	ForView view;
	if (info.ref2) {
		auto &right = chunk.data[info.ref2->Index()];
		if (col.GetVectorType() != right.GetVectorType() || !ResolveNarrowCol(right, r) || l.type != r.type) {
			return false;
		}
	} else {
		const auto &constant = info.constant->GetValue();
		if (constant.IsNull() || constant.type().InternalType() != pt) {
			return false;
		}
		if (l_for) {
			if (!TryResolveForView(col, info.op, constant, view)) {
				return false;
			}
		}
	}

	const bool have_sel = sel && sel->IsSet();
	const idx_t span = have_sel ? chunk.size() : count;
	if (span > STANDARD_VECTOR_SIZE) {
		return false;
	}

	SelectionResult &t = bitmap_sel ? *bitmap_sel : tmp_sel1;
	auto t_bm = reinterpret_cast<validity_t *>(t.PrepareBitmap(span));
	if (info.ref2) {
		FillNarrowCol(l, r, info.op, span, t_bm);
	} else if (!l_for) {
		const auto &constant = info.constant->GetValue();
		DispatchFlatCmpToBitmap(pt, info.op, col, span, l.validity, t_bm,
		                        [&](auto tag) { return constant.GetValueUnsafe<decltype(tag)>(); });
	} else {
		FillConstView(view, info.op, span, t_bm);
	}
	result = EmitBitmapSelection(t, t_bm, span, sel, count, bitmap_sel, true_sel, false_sel, tmp_sel2, tmp_sel3);
	return true;
}

} // namespace duckdb
