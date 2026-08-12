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
#include "duckdb/common/vector_operations/comparison_bitmap.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

namespace duckdb {
//! Extract the ref/constant (or ref/ref) shape of a comparison the bitmap kernels can evaluate
inline bool TryGetBitmapComparisonInfo(const Expression &expr, BitmapComparisonInfo &info) {
	if (expr.IsVolatile() || expr.CanThrow() || !BoundComparisonExpression::IsComparison(expr)) {
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
	const auto pt = info.ref->GetReturnType().InternalType();
	if (!BitmapCmpTypeSupported(pt)) {
		return false;
	}
	if (info.ref2) {
		// binding casts mismatched operands, so a bare ref/ref pair always shares its physical type
		D_ASSERT(info.ref2->GetReturnType().InternalType() == pt);
		return true;
	}
	const auto &value = info.constant->GetValue();
	return !value.IsNull() && value.type().InternalType() == pt;
}
inline bool IsBitmapSelectCandidate(const Expression &expr) { // comparison or conjunction of comparisons
#if !DUCKDB_AUTOVEC
	return false; // bitmap kernels are not compiled in
#else
	if (expr.GetExpressionType() == ExpressionType::CONJUNCTION_AND ||
	    expr.GetExpressionType() == ExpressionType::CONJUNCTION_OR) {
		for (auto &child : expr.Cast<BoundConjunctionExpression>().GetChildren()) {
			if (!IsBitmapSelectCandidate(*child)) {
				return false;
			}
		}
		return true;
	}
	BitmapComparisonInfo info;
	return TryGetBitmapComparisonInfo(expr, info);
#endif
}
#if DUCKDB_AUTOVEC
// target attr: only reachable behind CpuBenefitsFromAutoVec(); lets the bitmap kernels inline here
DUCKDB_AUTOVEC_TARGET inline bool SelectComparisonFromChunk(ExecuteFunctionState &fstate, DataChunk &chunk,
                                                            const SelectionVector *sel, idx_t count,
                                                            SelectionResult *bitmap_sel, SelectionVector *true_sel,
                                                            SelectionVector *false_sel, idx_t &result) {
	if (false_sel && !bitmap_sel) { // a false side needs the complement: leave it to the classic select
		return false;
	}
	const auto &info = fstate.cmp_info;
	auto &col_in = chunk.data[info.ref->Index()]; // dense compare reads the flat input directly
	// A sliced FOR input compares densely on the dictionary child: narrow payload never widens just to be filtered.
	const SelectionVector *slice_sel = nullptr;
	reference<Vector> lref(col_in);
	if (col_in.GetVectorType() == VectorType::DICTIONARY_VECTOR && ForVector::IsFor(DictionaryVector::Child(col_in))) {
		if (info.ref2) {
			auto &right = chunk.data[info.ref2->Index()];
			if (right.GetVectorType() != VectorType::DICTIONARY_VECTOR ||
			    DictionaryVector::SelVector(right).data() != DictionaryVector::SelVector(col_in).data()) {
				return false; // only one shared slice reads through to both children
			}
		}
		slice_sel = &DictionaryVector::SelVector(col_in);
		lref = DictionaryVector::Child(col_in);
	}
	Vector &col = lref.get();
	auto right_side = [&]() -> Vector & {
		auto &right = chunk.data[info.ref2->Index()];
		return slice_sel ? DictionaryVector::Child(right) : right;
	};
	if (!ForVector::IsFor(col) && !AutoVecCountPaysOff(count)) { // A FOR payload is worth comparing always
		return false; // Only non-FOR inputs bails for autovec if there are too few selected tuples anymore
	}
	auto pt = col.GetType().InternalType();
	uint64_t stored_constant = 0;
	bool stored = false;
	bool stored_pair = false;
	if (ForVector::IsFor(col)) {
		if (info.ref2) {
			// two payloads of one width compare as stored: the values are absolute and non-negative
			auto &right = right_side();
			stored_pair = ForVector::IsFor(right) && ForVector::StoredType(right) == ForVector::StoredType(col);
		} else {
			stored = ForVector::TryStoredConstant(col, info.constant->GetValue(), stored_constant);
		}
		if (stored || stored_pair) {
			pt = ForVector::StoredType(col);
		} else if (slice_sel) {
			return false; // no narrow compare to run: leave the sliced input to the classic path
		} else {
			ForVector::Widen(col);
		}
	}
	if (col.GetVectorType() != VectorType::FLAT_VECTOR && !stored && !stored_pair) { // sliced inputs fall back
		return false;
	}
	if (!BitmapCmpTypeSupported(pt)) {
		return false;
	}
	optional_ptr<Vector> col2;
	if (info.ref2) {
		auto &right = right_side();
		if (!stored_pair) {
			ForVector::Widen(right); // a lone narrow side compares at the logical width
		}
		if (right.GetVectorType() != VectorType::FLAT_VECTOR && !stored_pair) {
			return false;
		}
		col2 = right;
	} else if (!stored) { // a stored constant has already been range-checked against the payload
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
	if (have_sel && !stored && !stored_pair && !DenseAutoVecPaysOff(count, span, GetTypeIdSize(pt))) {
		return false; // FOR does not bail here (generic would widen the column which is slower)
	}
	SelectionResult &t = bitmap_sel ? *bitmap_sel : fstate.tmp_sel1; // true-side bitmap
	auto t_bm = t.PrepareBitmap(span);
	const idx_t cmp_span = slice_sel ? col.size() : span; // the children compare over their own domain
	uint64_t child_bm[STANDARD_VECTOR_SIZE / 64];
	auto cmp_bm = slice_sel ? child_bm : t_bm;
	if (cmp_span > STANDARD_VECTOR_SIZE) {
		return false;
	}
	auto &lvalidity = col.Buffer().GetValidityMask();
	const validity_t *lvalidity_data = lvalidity.CanHaveNull() ? lvalidity.GetData() : nullptr;
	const validity_t *rvalidity_data = nullptr;
	const_data_ptr_t rdata = nullptr;
	if (col2) {
		auto &rvalidity = col2->Buffer().GetValidityMask();
		rvalidity_data = rvalidity.CanHaveNull() ? rvalidity.GetData() : nullptr;
		rdata = FlatVector::GetDataUnsafe(*col2);
	}
	DispatchFlatCmpToBitmap(pt, info.op, FlatVector::GetDataUnsafe(col), rdata, cmp_span, lvalidity_data,
	                        rvalidity_data, cmp_bm, [&](auto tag) {
		                        using T = decltype(tag);
		                        if (stored) {
			                        return static_cast<T>(stored_constant);
		                        }
		                        return col2 ? T(0) : info.constant->GetValue().GetValueUnsafe<T>();
	                        });
	if (slice_sel) { // read the child bitmap through the slice into the chunk-domain bitmap
		for (idx_t i = 0; i < span; i += 64) {
			uint64_t word = 0;
			const idx_t next = MinValue<idx_t>(span - i, 64);
			for (idx_t j = 0; j < next; j++) {
				const auto cidx = slice_sel->get_index_unsafe(i + j);
				word |= ((child_bm[cidx >> 6] >> (cidx & 63)) & 1ULL) << j;
			}
			t_bm[i >> 6] = word;
		}
	}
	if (stored || stored_pair) {
		ForVector::MarkExploited(col); // the narrow compare paid off: let the producer keep emitting FOR
	}
	if (stored_pair) {
		ForVector::MarkExploited(*col2);
	}
	if (have_sel) { // AND input selection into the comparison bitmap
		fstate.tmp_sel2.Initialize(*sel);
		fstate.tmp_sel2.ToBitmap(count, span);
		result = t.Intersect(fstate.tmp_sel2, span, count, span);
	} else {
		result = BitmapPopcount(t_bm, span);
	}
	if (!bitmap_sel && true_sel) { // materialize only for plain selvec callers
		BitmapToSelectionVector(t_bm, span, *true_sel);
	}
	return true;
}
#endif // DUCKDB_AUTOVEC

} // namespace duckdb
