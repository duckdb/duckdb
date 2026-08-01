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
	if (raw_op == ExpressionType::COMPARE_DISTINCT_FROM) { // NULL-aware distinct cannot use validity-AND semantics
		return false;
	}
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
		if (raw_op == ExpressionType::COMPARE_NOT_DISTINCT_FROM) { // col-col NULL=NULL must not map to '='
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
		return true;
	}
	const auto &value = info.constant->GetValue();
	return !value.IsNull() && value.type().InternalType() == pt;
}

inline bool IsBitmapSelectCandidate(const Expression &expr) { // comparison or conjunction of comparisons
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND ||
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			if (!IsBitmapSelectCandidate(*child)) {
				return false;
			}
		}
		return true;
	}
	return IsBitmapComparisonCandidate(expr);
}

inline bool SelectComparisonFromChunk(const BitmapComparisonInfo &info, DataChunk &chunk, const SelectionVector *sel,
                                      idx_t count, SelectionResult *bitmap_sel, SelectionVector *true_sel,
                                      SelectionVector *false_sel, SelectionResult &tmp_sel1, SelectionResult &tmp_sel2,
                                      SelectionResult &tmp_sel3, idx_t &result) {
	auto &col = chunk.data[info.ref->Index()]; // dense compare reads the flat input directly
	const auto pt = col.GetType().InternalType();
	if (col.GetVectorType() != VectorType::FLAT_VECTOR || !BitmapCmpTypeSupported(pt)) { // sliced inputs fall back
		return false;
	}
	optional_ptr<Vector> col2;
	if (info.ref2) {
		auto &right = chunk.data[info.ref2->Index()];
		if (right.GetVectorType() != VectorType::FLAT_VECTOR) {
			return false;
		}
		col2 = right;
	} else {
		const auto &constant = info.constant->GetValue();
		if (constant.IsNull() || constant.type().InternalType() != pt) {
			return false;
		}
	}

	const bool have_sel = sel && sel->IsSet();
	const idx_t span = have_sel ? chunk.size() : count; // selected indices still span the chunk domain
	if (span > STANDARD_VECTOR_SIZE) {                  // bitmap scratch is vector-sized
		return false;
	}
	if (have_sel && !DenseAutoVecPaysOff(count, span, GetTypeIdSize(pt))) {
		return false;
	}

	SelectionResult &t = bitmap_sel ? *bitmap_sel : tmp_sel1; // true-side bitmap
	auto t_bm = reinterpret_cast<validity_t *>(t.PrepareBitmap(span));
	auto &lvalidity = FlatVector::Validity(col);
	const validity_t *lvalidity_data = lvalidity.CanHaveNull() ? lvalidity.GetData() : nullptr;
	if (col2) {
		auto &rvalidity = FlatVector::Validity(*col2);
		const validity_t *rvalidity_data = rvalidity.CanHaveNull() ? rvalidity.GetData() : nullptr;
		DispatchFlatColCmpToBitmap(pt, info.op, col, *col2, span, lvalidity_data, rvalidity_data, t_bm);
	} else {
		const auto &constant = info.constant->GetValue();
		DispatchFlatCmpToBitmap(pt, info.op, col, span, lvalidity_data, t_bm,
		                        [&](auto tag) { return constant.GetValueUnsafe<decltype(tag)>(); });
	}

	validity_t *f_bm = nullptr;
	if (false_sel && !bitmap_sel) { // complement before input-selection intersection
		f_bm = tmp_sel3.Complement(t, span);
	}

	if (have_sel) { // AND input selection into the comparison bitmap
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
	if (!bitmap_sel && true_sel) { // materialize only for plain selvec callers
		BitmapToSelectionVector(t_bm, span, *true_sel);
	}
	return true;
}

} // namespace duckdb
