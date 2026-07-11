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
#include "duckdb/common/vector/dictionary_vector.hpp"
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

//! A resolved narrow operand: raw payload pointer, its narrow physical type, and validity words (nullptr when the
//! column cannot be NULL). Resolved uniformly from a bare FLAT or FOR vector (or a dictionary child), so the
//! column-vs-column kernels never branch on representation: a FLAT column uses its own physical type, a FOR column
//! its stored narrow type. Both are just a contiguous typed array to the kernels.
struct NarrowCol {
	const_data_ptr_t data = nullptr;
	PhysicalType type = PhysicalType::INVALID;
	const validity_t *validity = nullptr;
};

//! Resolve a bare FLAT or FOR vector into a NarrowCol. Returns false for any other vector type.
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

//! Unwrap one DICTIONARY layer whose child is a bare FLAT or FOR vector (a selective scan slice). Returns the child
//! and its selection, or nullptr if `v` is not such a dictionary.
inline const Vector *TryGetDictChild(const Vector &v, const SelectionVector *&sel) {
	if (v.GetVectorType() != VectorType::DICTIONARY_VECTOR) {
		return nullptr;
	}
	auto &child = DictionaryVector::Child(v);
	const auto cvt = child.GetVectorType();
	if (cvt == VectorType::FOR_VECTOR) {
		sel = &DictionaryVector::SelVector(v);
		return &child;
	}
	return nullptr;
}

//! Column-vs-constant into a true bitmap via a resolved ForView (which unifies FLAT thin-int and FOR: a FLAT column
//! is a FOR view with offset 0). Reads the raw payload on raw pointers -- no Vector, no allocation. Range-proven
//! uniform results short-circuit.
inline void FillConstView(const ForView &view, ExpressionType op, idx_t span, validity_t *t_bm) {
	const validity_t *validity = view.original_validity ? view.original_validity->GetData() : nullptr;
	if (view.always_false || view.always_true) {
		WriteConstantBitmap(view.always_true, span, validity, t_bm);
		return;
	}
	DispatchBitmapType(view.narrow_type, span, [&](auto tag) {
		using T = decltype(tag);
		const auto data = reinterpret_cast<const T *>(view.data);
		// float/double branches of DispatchBitmapType are dead for a narrow view but must compile, so static_cast
		const auto constant = static_cast<T>(view.rewritten_constant);
		DispatchBitmapCmpOp<T>(
		    op, [&](auto cmp) { NarrowCmpToBitmap<T, decltype(cmp)>(data, constant, span, validity, t_bm); });
	});
}

//! Dense column-vs-column over two resolved narrow operands of the same narrow type into a true bitmap. Shared by
//! bare FLAT-vs-FLAT and FOR-vs-FOR, and by the dense-dict path (over the child span).
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

//! Sparse column-vs-column: gather-compare only the `count` selected values (work ∝ count, not the child span).
//! Writes a byte-per-row 0/1 result. Shared by dict-over-FLAT and dict-over-FOR.
inline void GatherNarrowCol(const NarrowCol &l, const NarrowCol &r, ExpressionType op, const SelectionVector &dsel,
                            idx_t count, uint8_t *cmp) {
	DispatchBitmapType(l.type, count, [&](auto tag) {
		using T = decltype(tag);
		const auto ld = reinterpret_cast<const T *>(l.data);
		const auto rd = reinterpret_cast<const T *>(r.data);
		DispatchBitmapCmpOp<T>(op, [&](auto cmpop) {
			for (idx_t i = 0; i < count; i++) {
				const auto c = dsel.get_index(i);
				cmp[i] = decltype(cmpop)::Operation(ld[c], rd[c]);
			}
		});
	});
	// validity folded in only when a side can be NULL (all-valid path stays free of validity machinery)
	if (l.validity || r.validity) {
		for (idx_t i = 0; i < count; i++) {
			const auto c = dsel.get_index(i);
			if ((l.validity && !((l.validity[c >> 6] >> (c & 63)) & 1)) ||
			    (r.validity && !((r.validity[c >> 6] >> (c & 63)) & 1))) {
				cmp[i] = 0;
			}
		}
	}
}

//! Sparse column-vs-constant: gather-compare the `count` selected narrow values against the resolved constant.
inline void GatherConstView(const ForView &view, ExpressionType op, const SelectionVector &dsel, idx_t count,
                            uint8_t *cmp) {
	if (view.always_false || view.always_true) {
		for (idx_t i = 0; i < count; i++) {
			cmp[i] = view.always_true ? 1 : 0;
		}
	} else {
		DispatchBitmapType(view.narrow_type, count, [&](auto tag) {
			using T = decltype(tag);
			const auto data = reinterpret_cast<const T *>(view.data);
			const auto constant = static_cast<T>(view.rewritten_constant);
			DispatchBitmapCmpOp<T>(op, [&](auto cmpop) {
				for (idx_t i = 0; i < count; i++) {
					cmp[i] = decltype(cmpop)::Operation(data[dsel.get_index(i)], constant);
				}
			});
		});
	}
	if (view.original_validity && view.original_validity->CanHaveNull()) {
		for (idx_t i = 0; i < count; i++) {
			if (!view.original_validity->RowIsValid(dsel.get_index(i))) {
				cmp[i] = 0;
			}
		}
	}
}

//! Shared emit tail: given a filled true-bitmap `t` of length `len` (== count in dict/count space, == span in the
//! dense-vector space), take the false side (complement) if asked, fold in any input selection as a bitmap, and
//! materialize the requested outputs (result bitmap, and/or true/false selection vectors). Returns surviving count.
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

	// A dictionary over a dense FLAT/FOR child (a selective scan slice): resolve the operand(s) on the child, then
	// either gather-compare only the selected values (∝ count, sparse dicts) or compare the whole child once (∝
	// child span, SIMD) and probe the result through the dictionary selection. Output is a subset of the dictionary
	// selection, in logical-row space. Both col-col operands must share the child representation and selection.
	{
		const SelectionVector *ldsel = nullptr;
		auto lchild = TryGetDictChild(col, ldsel);
		if (lchild) {
			const idx_t child_len = lchild->size();
			if (child_len > STANDARD_VECTOR_SIZE || count > STANDARD_VECTOR_SIZE) {
				return false;
			}
			// resolve operand(s) on the dense child; narrow_size drives the gather-vs-dense crossover
			NarrowCol l, r;
			ForView view;
			idx_t narrow_size;
			if (info.ref2) {
				const SelectionVector *rdsel = nullptr;
				auto rchild = TryGetDictChild(chunk.data[info.ref2->Index()], rdsel);
				if (!rchild || !rdsel || !SelectionVector::SameSelection(*ldsel, *rdsel) ||
				    lchild->GetVectorType() != rchild->GetVectorType() || !ResolveNarrowCol(*lchild, l) ||
				    !ResolveNarrowCol(*rchild, r) || l.type != r.type) {
					return false;
				}
				narrow_size = GetTypeIdSize(l.type);
			} else {
				// col-const: only a FOR child resolves here (constant rebased into the stored domain); a FLAT-child
				// dict-const bails to the generic select, which already gathers ∝ count off the flat child
				const auto &constant = info.constant->GetValue();
				if (constant.IsNull() || constant.type().InternalType() != pt ||
				    !TryResolveForView(*lchild, info.op, constant, view) || view.kind != ForView::Kind::FOR) {
					return false;
				}
				narrow_size = GetTypeIdSize(view.narrow_type);
			}
			const bool sparse = PreferSelectionGather(count, child_len, narrow_size);
			uint8_t cmp[STANDARD_VECTOR_SIZE];
			if (sparse) {
				if (info.ref2) {
					GatherNarrowCol(l, r, info.op, *ldsel, count, cmp);
				} else {
					GatherConstView(view, info.op, *ldsel, count, cmp);
				}
			} else {
				// dense comparison over the child -> scratch bitmap (child-position space), then probe
				static constexpr idx_t NWORDS = (STANDARD_VECTOR_SIZE + 63) / 64;
				validity_t child_bm[NWORDS];
				if (info.ref2) {
					FillNarrowCol(l, r, info.op, child_len, child_bm);
				} else {
					FillConstView(view, info.op, child_len, child_bm);
				}
				const auto child_bytes = reinterpret_cast<const uint8_t *>(child_bm);
				for (idx_t i = 0; i < count; i++) {
					const auto c = ldsel->get_index(i);
					cmp[i] = (child_bytes[c >> 3] >> (c & 7)) & 1;
				}
			}
			// Plain filter case (a true selection, no bitmap/false output, no active input selection): emit the
			// surviving logical rows straight from cmp[] into true_sel. These dictionary chunks are typically small
			// (a sparse scan slice), so skipping the bitmap round-trip (pack -> popcount -> unpack) per chunk matters.
			if (true_sel && !bitmap_sel && !false_sel && !(sel && sel->IsSet())) {
				idx_t k = 0;
				for (idx_t i = 0; i < count; i++) {
					if (cmp[i]) {
						true_sel->set_index(k++, i);
					}
				}
				result = k;
				return true;
			}
			SelectionResult &t = bitmap_sel ? *bitmap_sel : tmp_sel1;
			auto t_bm = reinterpret_cast<validity_t *>(t.PrepareBitmap(count));
			PackBoolsToBitmap(cmp, count, t_bm);
			result =
			    EmitBitmapSelection(t, t_bm, count, sel, count, bitmap_sel, true_sel, false_sel, tmp_sel2, tmp_sel3);
			return true;
		}
	}

	// Bare dense narrow payload: FLAT or FOR. Dictionary/sliced/constant shapes are handled above or bail to the
	// generic path. Comparison runs DENSE over the payload (ignoring any input selection), which is then AND-ed in
	// lazily as a bitmap at the emit boundary.
	NarrowCol l;
	if (!ResolveNarrowCol(col, l)) {
		return false;
	}
	const bool l_for = col.GetVectorType() == VectorType::FOR_VECTOR;

	// Resolve and validate BOTH operands (everything that can bail) BEFORE touching any bitmap state: `bitmap_sel`
	// aliases the caller's accumulator, so a PrepareBitmap followed by a bail would corrupt it (a dense conjunction
	// child that bails here must leave the accumulator untouched for the generic fallback).
	NarrowCol r;
	ForView view;
	bool flat_const = false;
	if (info.ref2) {
		auto &right = chunk.data[info.ref2->Index()];
		// both sides must share a representation (both FLAT, or both FOR) and narrow width: mixing or differing
		// widths would need to widen one side, and FOR stored values are only order-comparable at equal width
		if (col.GetVectorType() != right.GetVectorType() || !ResolveNarrowCol(right, r) || l.type != r.type) {
			return false;
		}
	} else {
		const auto &constant = info.constant->GetValue();
		if (constant.IsNull() || constant.type().InternalType() != pt) {
			return false;
		}
		if (l_for) {
			// FOR: constant rebased into the stored domain (ForView); an unhandled FOR shape bails
			if (!TryResolveForView(col, info.op, constant, view) || view.kind != ForView::Kind::FOR) {
				return false;
			}
		} else {
			flat_const = true;
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

	SelectionResult &t = bitmap_sel ? *bitmap_sel : tmp_sel1;
	auto t_bm = reinterpret_cast<validity_t *>(t.PrepareBitmap(span));
	if (info.ref2) {
		FillNarrowCol(l, r, info.op, span, t_bm);
	} else if (flat_const) {
		// FLAT: compare the raw payload against the raw constant directly (any bitmap-supported width)
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
