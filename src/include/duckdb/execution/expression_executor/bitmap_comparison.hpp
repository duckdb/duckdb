//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor/bitmap_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/bitmap_selection_vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_result.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/for_view.hpp"
#include "duckdb/common/vector_operations/comparison_bitmap.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

struct BitmapComparisonInfo {
	optional_ptr<const BoundReferenceExpression> ref;
	optional_ptr<const BoundConstantExpression> constant;
	ExpressionType op;
};

inline bool TryGetBitmapComparisonInfo(const Expression &expr, BitmapComparisonInfo &info) {
	if (!BoundComparisonExpression::IsComparison(expr)) {
		return false;
	}
	auto op = expr.GetExpressionType();
	if (op == ExpressionType::COMPARE_DISTINCT_FROM) {
		return false;
	}
	if (op == ExpressionType::COMPARE_NOT_DISTINCT_FROM) {
		op = ExpressionType::COMPARE_EQUAL;
	}
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
	} else {
		return false;
	}
	return true;
}

inline bool IsBitmapComparisonCandidate(const Expression &expr) {
	if (expr.IsVolatile() || expr.CanThrow()) {
		return false;
	}
	BitmapComparisonInfo info;
	if (!TryGetBitmapComparisonInfo(expr, info)) {
		return false;
	}
	const auto &value = info.constant->GetValue();
	const auto pt = info.ref->GetReturnType().InternalType();
	return !value.IsNull() && BitmapCmpTypeSupported(pt) && value.type().InternalType() == pt;
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

//! FOR view technique: fill the true-side bitmap by running the comparison on the narrow stored payload that
//! underlies a (bare) FOR vector, reusing the same bitmap kernels as the flat path. The rewritten constant and
//! validity come from the ForView. Returns false if `col` is not a FOR-viewable column.
inline bool FillForComparisonBitmap(const Vector &col, ExpressionType op, const Value &constant, idx_t span,
                                    validity_t *t_bm) {
	// only bare FOR vectors: other non-flat shapes (constant, dictionary, ...) must fall through to generic
	if (col.GetVectorType() != VectorType::FOR_VECTOR) {
		return false;
	}
	ForView view;
	if (!TryResolveForView(col, op, constant, view) || view.kind != ForView::Kind::FOR) {
		return false;
	}
	const validity_t *validity_data = view.original_validity ? view.original_validity->GetData() : nullptr;
	if (view.always_false || view.always_true) {
		WriteConstantBitmap(view.always_true, span, validity_data, t_bm);
		return true;
	}
	switch (view.narrow_type) {
	case PhysicalType::UINT8:
		DispatchCmpToBitmap<uint8_t>(op, reinterpret_cast<const uint8_t *>(view.data),
		                             UnsafeNumericCast<uint8_t>(view.rewritten_constant), span, validity_data, t_bm);
		return true;
	case PhysicalType::UINT16:
		DispatchCmpToBitmap<uint16_t>(op, reinterpret_cast<const uint16_t *>(view.data),
		                              UnsafeNumericCast<uint16_t>(view.rewritten_constant), span, validity_data, t_bm);
		return true;
	case PhysicalType::UINT32:
		DispatchCmpToBitmap<uint32_t>(op, reinterpret_cast<const uint32_t *>(view.data),
		                              UnsafeNumericCast<uint32_t>(view.rewritten_constant), span, validity_data, t_bm);
		return true;
	default:
		return false;
	}
}

//! General `ref <op> const` comparison selection fast path. Evaluates the comparison DENSELY over the input into a
//! bitmap (branchless/autovec), then combines lazily: any input selection is AND-ed in as a bitmap, and the result
//! is emitted to whatever the caller wants (a result bitmap, or a true and/or false selection vector). Returns false
//! (nothing written) for shapes it does not handle, so the caller falls through to generic selection.
inline bool SelectComparisonFromChunk(const BoundFunctionExpression &expr, DataChunk &chunk, const SelectionVector *sel,
                                      idx_t count, SelectionResult *bitmap_sel, SelectionVector *true_sel,
                                      SelectionVector *false_sel, SelectionResult &tmp_sel1, SelectionResult &tmp_sel2,
                                      SelectionResult &tmp_sel3, idx_t &result) {
	// when a bitmap output is requested, true_sel aliases its flat view (see ExpressionExecutor::Select): the
	// comparison result lands in bitmap_sel and true_sel/false_sel are not materialized separately
	BitmapComparisonInfo info;
	if (!TryGetBitmapComparisonInfo(expr, info)) {
		return false;
	}
	auto &constant = info.constant->GetValue();
	auto &col = chunk.data[info.ref->Index()];
	const auto pt = col.GetType().InternalType();
	// a bound comparison has both sides at the same type, so the constant needs no cast
	if (constant.IsNull() || constant.type().InternalType() != pt) {
		return false;
	}
	// Only FLAT bitmap-comparable columns and bare FOR vectors are handled here; anything else bails before
	// touching any bitmap state (identical early-out to the autovec path, extended only to admit FOR).
	const bool col_flat = col.GetVectorType() == VectorType::FLAT_VECTOR;
	const bool col_for = col.GetVectorType() == VectorType::FOR_VECTOR;
	if ((col_flat && !BitmapCmpTypeSupported(pt)) || (!col_flat && !col_for)) {
		return false;
	}

	const bool have_sel = sel && sel->IsSet();
	// dense over the whole vector when a selection is active (selvec indices span it), else over count
	const idx_t span = have_sel ? chunk.size() : count;

	// dense comparison -> bitmap (the true side), in the caller's bitmap when one is requested else a scratch
	SelectionResult &t = bitmap_sel ? *bitmap_sel : tmp_sel1;
	auto t_bm = reinterpret_cast<validity_t *>(t.PrepareBitmap(span));
	if (col_flat) {
		auto &validity = FlatVector::Validity(col);
		const validity_t *validity_data = validity.CanHaveNull() ? validity.GetData() : nullptr;
		DispatchFlatCmpToBitmap(pt, info.op, col, span, validity_data, t_bm,
		                        [&](auto tag) { return constant.GetValueUnsafe<decltype(tag)>(); });
	} else if (!FillForComparisonBitmap(col, info.op, constant, span, t_bm)) {
		// FOR vector whose narrow view does not resolve (e.g. wider stored type): fall through to generic
		return false;
	}

	// the false side is the complement; take it before folding in the input
	validity_t *f_bm = nullptr;
	if (false_sel && !bitmap_sel) {
		f_bm = tmp_sel3.Complement(t, span);
	}

	// AND the input selection into both sides via SelectionResult (ToBitmap: index->bitmap, Intersect: AND+popcount)
	if (have_sel) {
		tmp_sel2.Initialize(*sel);
		tmp_sel2.ToBitmap(count, span);
		result = t.Intersect(tmp_sel2, span, count, span);
		if (f_bm) {
			tmp_sel3.Intersect(tmp_sel2, span, count, span);
		}
	} else {
		result = BitmapPopcount(t_bm, span);
	}

	if (f_bm) {
		BitmapToSelectionVector(f_bm, span, *false_sel);
	}
	if (!bitmap_sel && true_sel) {
		BitmapToSelectionVector(t_bm, span, *true_sel);
	}
	return true;
}

} // namespace duckdb
