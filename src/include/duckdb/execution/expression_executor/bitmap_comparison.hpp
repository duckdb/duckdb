//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor/bitmap_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_result.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/for_view.hpp"
#include "duckdb/common/numeric_utils.hpp"
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

template <class ConstGetter>
inline bool SelectFlatComparisonToBitmap(const Vector &flat, ExpressionType op, idx_t count, SelectionResult &true_sel,
                                         ConstGetter get_const, idx_t &result) {
	const auto pt = flat.GetType().InternalType();
	if (flat.GetVectorType() != VectorType::FLAT_VECTOR || !BitmapCmpTypeSupported(pt)) {
		return false;
	}
	auto &validity = FlatVector::Validity(flat);
	const validity_t *validity_data = validity.CanHaveNull() ? validity.GetData() : nullptr;
	auto bitmap = reinterpret_cast<validity_t *>(true_sel.PrepareBitmap(count));
	DispatchFlatCmpToBitmap(pt, op, flat, count, validity_data, bitmap, std::move(get_const));
	result = BitmapPopcount(bitmap, count);
	return true;
}

//! FOR-vector fast path: compare the narrow stored payload against the rewritten constant.
inline bool SelectForComparisonToBitmap(const Vector &col, ExpressionType op, const Value &constant, idx_t count,
                                        SelectionResult &true_sel, idx_t &result) {
	ForView view;
	if (!TryResolveForView(col, op, constant, view) || view.kind != ForView::Kind::FOR) {
		return false;
	}
	const auto validity = view.original_validity ? view.original_validity->GetData() : nullptr;
	auto bitmap = reinterpret_cast<validity_t *>(true_sel.PrepareBitmap(count));
	if (view.always_false || view.always_true) {
		WriteConstantBitmap(view.always_true, count, validity, bitmap);
	} else {
		switch (view.narrow_type) {
		case PhysicalType::UINT8:
			DispatchCmpToBitmap<uint8_t>(op, reinterpret_cast<const uint8_t *>(view.data),
			                             UnsafeNumericCast<uint8_t>(view.rewritten_constant), count, validity, bitmap);
			break;
		case PhysicalType::UINT16:
			DispatchCmpToBitmap<uint16_t>(op, reinterpret_cast<const uint16_t *>(view.data),
			                              UnsafeNumericCast<uint16_t>(view.rewritten_constant), count, validity,
			                              bitmap);
			break;
		case PhysicalType::UINT32:
			DispatchCmpToBitmap<uint32_t>(op, reinterpret_cast<const uint32_t *>(view.data),
			                              UnsafeNumericCast<uint32_t>(view.rewritten_constant), count, validity,
			                              bitmap);
			break;
		default:
			throw InternalException("Unsupported FOR stored type for bitmap select");
		}
	}
	result = BitmapPopcount(bitmap, count);
	return true;
}

//! Fast path for `flat_ref <cmp> const`: evaluate straight from the input chunk into a bitmap, skipping
//! intermediate vector materialization. Returns false for anything it does not handle.
inline bool TrySelectComparisonFromChunk(const BoundFunctionExpression &expr, DataChunk &chunk, idx_t count,
                                         SelectionResult &true_sel, idx_t &result) {
	BitmapComparisonInfo info;
	if (!TryGetBitmapComparisonInfo(expr, info)) {
		return false;
	}
	auto &constant = info.constant->GetValue();
	auto &col = chunk.data[info.ref->Index()];
	const auto pt = col.GetType().InternalType();
	// A bound comparison has both sides at the same type, so the constant needs no cast.
	if (constant.IsNull() || constant.type().InternalType() != pt) {
		return false;
	}
	if (col.GetVectorType() == VectorType::FOR_VECTOR) {
		return SelectForComparisonToBitmap(col, info.op, constant, count, true_sel, result);
	}
	return SelectFlatComparisonToBitmap(
	    col, info.op, count, true_sel, [&](auto tag) { return constant.GetValueUnsafe<decltype(tag)>(); }, result);
}

} // namespace duckdb
