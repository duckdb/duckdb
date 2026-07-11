//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/expression_executor/for_bitmap_comparison.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/expression_executor/bitmap_comparison.hpp"

#include "duckdb/common/types/bitmap_selection_vector.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/selection_result.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/common/vector/dictionary_vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/common/vector/for_vector.hpp"
#include "duckdb/common/vector/for_view.hpp"

namespace duckdb {

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
	auto &child = DictionaryVector::Child(v);
	if (child.GetVectorType() == VectorType::FOR_VECTOR) {
		sel = &DictionaryVector::SelVector(v);
		return &child;
	}
	return nullptr;
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

inline bool SelectComparisonFromChunk(const BoundFunctionExpression &expr, DataChunk &chunk, const SelectionVector *sel,
                                      idx_t count, SelectionResult *bitmap_sel, SelectionVector *true_sel,
                                      SelectionVector *false_sel, SelectionResult &tmp_sel1, SelectionResult &tmp_sel2,
                                      SelectionResult &tmp_sel3, idx_t &result) {
	BitmapComparisonInfo info;
	if (!TryGetBitmapComparisonInfo(expr, info)) {
		return false;
	}
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
				const auto &constant = info.constant->GetValue();
				if (constant.IsNull() || constant.type().InternalType() != pt ||
				    !TryResolveForView(*lchild, info.op, constant, view) || view.kind != ForView::Kind::FOR) {
					return false;
				}
				narrow_size = GetTypeIdSize(view.narrow_type);
			}
			const bool sparse = PreferSelectionGather(logical_span, child_len, narrow_size);
			uint8_t cmp[STANDARD_VECTOR_SIZE];
			if (sparse) {
				if (info.ref2) {
					GatherNarrowCol(l, r, info.op, *ldsel, logical_span, cmp);
				} else {
					GatherConstView(view, info.op, *ldsel, logical_span, cmp);
				}
			} else {
				static constexpr idx_t NWORDS = (STANDARD_VECTOR_SIZE + 63) / 64;
				validity_t child_bm[NWORDS];
				if (info.ref2) {
					FillNarrowCol(l, r, info.op, child_len, child_bm);
				} else {
					FillConstView(view, info.op, child_len, child_bm);
				}
				const auto child_bytes = reinterpret_cast<const uint8_t *>(child_bm);
				for (idx_t i = 0; i < logical_span; i++) {
					const auto c = ldsel->get_index(i);
					cmp[i] = (child_bytes[c >> 3] >> (c & 7)) & 1;
				}
			}
			if (true_sel && !bitmap_sel && !false_sel && !have_sel) {
				idx_t k = 0;
				for (idx_t i = 0; i < logical_span; i++) {
					if (cmp[i]) {
						true_sel->set_index(k++, i);
					}
				}
				result = k;
				return true;
			}
			SelectionResult &t = bitmap_sel ? *bitmap_sel : tmp_sel1;
			auto t_bm = reinterpret_cast<validity_t *>(t.PrepareBitmap(logical_span));
			PackBoolsToBitmap(cmp, logical_span, t_bm);
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
	bool flat_const = false;
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
			if (!TryResolveForView(col, info.op, constant, view) || view.kind != ForView::Kind::FOR) {
				return false;
			}
		} else {
			flat_const = true;
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
	} else if (flat_const) {
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
