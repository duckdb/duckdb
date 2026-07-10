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
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector/for_view.hpp"
#include "duckdb/common/vector_operations/comparison_bitmap.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {

struct BitmapComparisonInfo {
	optional_ptr<const BoundReferenceExpression> ref;
	//! exactly one of `constant` (ref <op> const) or `ref2` (ref <op> ref) is set
	optional_ptr<const BoundConstantExpression> constant;
	optional_ptr<const BoundReferenceExpression> ref2;
	ExpressionType op;
};

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

//! FOR column-vs-constant: fill the true bitmap from the narrow stored payload via a resolved ForView, reusing the
//! flat bitmap kernels on raw pointers -- no Vector, no allocation. Range-proven uniform results short-circuit.
inline void FillForConstView(const ForView &view, ExpressionType op, idx_t span, validity_t *t_bm) {
	const validity_t *validity = view.original_validity ? view.original_validity->GetData() : nullptr;
	if (view.always_false || view.always_true) {
		WriteConstantBitmap(view.always_true, span, validity, t_bm);
		return;
	}
	DispatchBitmapType(view.narrow_type, span, [&](auto tag) {
		using T = decltype(tag);
		const auto data = reinterpret_cast<const T *>(view.data);
		// narrow_type is always uint8/16/32 for FOR; the float/double branches of DispatchBitmapType are dead but
		// must compile, so use static_cast (the rewritten constant is range-checked into the narrow domain already)
		const auto constant = static_cast<T>(view.rewritten_constant);
		DispatchBitmapCmpOp<T>(
		    op, [&](auto cmp) { NarrowCmpToBitmap<T, decltype(cmp)>(data, constant, span, validity, t_bm); });
	});
}

//! FOR column-vs-column (both bare FOR of the same stored width): compare the narrow payloads directly on raw
//! pointers -- no Vector, no allocation. Differing widths / non-FOR shapes are rejected by the caller.
inline void FillForColView(const Vector &lcol, const Vector &rcol, ExpressionType op, idx_t span, validity_t *t_bm) {
	const auto ldata = FORVector::GetData(lcol);
	const auto rdata = FORVector::GetData(rcol);
	auto &lv = FORVector::Validity(lcol);
	auto &rv = FORVector::Validity(rcol);
	const validity_t *lval = lv.CanHaveNull() ? lv.GetData() : nullptr;
	const validity_t *rval = rv.CanHaveNull() ? rv.GetData() : nullptr;
	DispatchBitmapType(FORVector::GetStoredType(lcol), span, [&](auto tag) {
		using T = decltype(tag);
		DispatchBitmapCmpOp<T>(op, [&](auto cmp) {
			NarrowColCmpToBitmap<T, decltype(cmp)>(reinterpret_cast<const T *>(ldata), reinterpret_cast<const T *>(rdata),
			                                       span, lval, rval, t_bm);
		});
	});
}

//! General comparison selection fast path. Evaluates the comparison DENSELY over the input into a
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
	auto &col = chunk.data[info.ref->Index()];
	const auto pt = col.GetType().InternalType();
	if (!BitmapCmpTypeSupported(pt)) {
		return false;
	}
	// Left operand must be a dense narrow payload: bare FLAT or bare FOR. Dictionary/sliced/constant shapes bail to
	// the generic path (a sliced input would need a per-row remap to build the selection, ~3x slower than the
	// generic gather-select). FOR is admitted because its stored payload is dense, exactly like a flat column.
	const bool l_flat = col.GetVectorType() == VectorType::FLAT_VECTOR;
	const bool l_for = col.GetVectorType() == VectorType::FOR_VECTOR;
	if (!l_flat && !l_for) {
		return false;
	}
	optional_ptr<Vector> col2;
	if (info.ref2) {
		auto &right = chunk.data[info.ref2->Index()];
		// both sides must share a representation (both flat, or both FOR): mixing would require widening one side
		if ((l_flat && right.GetVectorType() != VectorType::FLAT_VECTOR) ||
		    (l_for && right.GetVectorType() != VectorType::FOR_VECTOR)) {
			return false;
		}
		col2 = right; // both sides of a bound comparison share pt
	} else {
		// a bound comparison has both sides at the same type, so the constant needs no cast
		const auto &constant = info.constant->GetValue();
		if (constant.IsNull() || constant.type().InternalType() != pt) {
			return false;
		}
	}
	// Resolve the FOR narrow view up front so an unhandled FOR shape bails before any bitmap state is touched.
	ForView lview;
	if (l_for) {
		if (col2) {
			if (FORVector::GetStoredType(col) != FORVector::GetStoredType(*col2)) {
				return false; // differing stored widths: no widening here
			}
		} else if (!TryResolveForView(col, info.op, info.constant->GetValue(), lview) ||
		           lview.kind != ForView::Kind::FOR) {
			return false;
		}
	}

	const bool have_sel = sel && sel->IsSet();
	// dense over the whole vector when a selection is active (selvec indices span it), else over count
	const idx_t span = have_sel ? chunk.size() : count;
	// the bitmap scratch holds exactly STANDARD_VECTOR_SIZE bits; larger inputs (e.g. a parquet dictionary filter
	// over a >2048-entry dictionary) must fall back to the generic select, which handles any size
	if (span > STANDARD_VECTOR_SIZE) {
		return false;
	}

	// dense comparison -> bitmap (the true side), in the caller's bitmap when one is requested else a scratch
	SelectionResult &t = bitmap_sel ? *bitmap_sel : tmp_sel1;
	auto t_bm = reinterpret_cast<validity_t *>(t.PrepareBitmap(span));
	if (l_for) {
		// FOR: run the flat kernels directly on the narrow stored payload (no Vector, no allocation)
		if (col2) {
			FillForColView(col, *col2, info.op, span, t_bm);
		} else {
			FillForConstView(lview, info.op, span, t_bm);
		}
	} else if (col2) {
		auto &lvalidity = FlatVector::Validity(col);
		const validity_t *lvalidity_data = lvalidity.CanHaveNull() ? lvalidity.GetData() : nullptr;
		auto &rvalidity = FlatVector::Validity(*col2);
		const validity_t *rvalidity_data = rvalidity.CanHaveNull() ? rvalidity.GetData() : nullptr;
		DispatchFlatColCmpToBitmap(pt, info.op, col, *col2, span, lvalidity_data, rvalidity_data, t_bm);
	} else {
		auto &lvalidity = FlatVector::Validity(col);
		const validity_t *lvalidity_data = lvalidity.CanHaveNull() ? lvalidity.GetData() : nullptr;
		const auto &constant = info.constant->GetValue();
		DispatchFlatCmpToBitmap(pt, info.op, col, span, lvalidity_data, t_bm,
		                        [&](auto tag) { return constant.GetValueUnsafe<decltype(tag)>(); });
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

	// materialize the plain selection(s) via the standard [0]-start primitive
	if (f_bm) {
		BitmapToSelectionVector(f_bm, span, *false_sel);
	}
	if (!bitmap_sel && true_sel) {
		BitmapToSelectionVector(t_bm, span, *true_sel);
	}
	return true;
}

} // namespace duckdb
