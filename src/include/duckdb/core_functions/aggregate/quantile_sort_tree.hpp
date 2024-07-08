//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/quantile_sort_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/core_functions/aggregate/quantile_helpers.hpp"
#include "duckdb/execution/merge_sort_tree.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include <algorithm>
#include <numeric>
#include <stdlib.h>
#include <utility>

namespace duckdb {

// Direct access
template <typename T>
struct QuantileDirect {
	using INPUT_TYPE = T;
	using RESULT_TYPE = T;

	inline const INPUT_TYPE &operator()(const INPUT_TYPE &x) const {
		return x;
	}
};

// Indirect access
template <typename T>
struct QuantileIndirect {
	using INPUT_TYPE = idx_t;
	using RESULT_TYPE = T;
	const RESULT_TYPE *data;

	explicit QuantileIndirect(const RESULT_TYPE *data_p) : data(data_p) {
	}

	inline RESULT_TYPE operator()(const idx_t &input) const {
		return data[input];
	}
};

// Composed access
template <typename OUTER, typename INNER>
struct QuantileComposed {
	using INPUT_TYPE = typename INNER::INPUT_TYPE;
	using RESULT_TYPE = typename OUTER::RESULT_TYPE;

	const OUTER &outer;
	const INNER &inner;

	explicit QuantileComposed(const OUTER &outer_p, const INNER &inner_p) : outer(outer_p), inner(inner_p) {
	}

	inline RESULT_TYPE operator()(const idx_t &input) const {
		return outer(inner(input));
	}
};

// Accessed comparison
template <typename ACCESSOR>
struct QuantileCompare {
	using INPUT_TYPE = typename ACCESSOR::INPUT_TYPE;
	const ACCESSOR &accessor;
	const bool desc;
	explicit QuantileCompare(const ACCESSOR &accessor_p, bool desc_p) : accessor(accessor_p), desc(desc_p) {
	}

	inline bool operator()(const INPUT_TYPE &lhs, const INPUT_TYPE &rhs) const {
		const auto lval = accessor(lhs);
		const auto rval = accessor(rhs);

		return desc ? (rval < lval) : (lval < rval);
	}
};

struct CastInterpolation {
	template <class INPUT_TYPE, class TARGET_TYPE>
	static inline TARGET_TYPE Cast(const INPUT_TYPE &src, Vector &result) {
		return Cast::Operation<INPUT_TYPE, TARGET_TYPE>(src);
	}
	template <typename TARGET_TYPE>
	static inline TARGET_TYPE Interpolate(const TARGET_TYPE &lo, const double d, const TARGET_TYPE &hi) {
		const auto delta = hi - lo;
		return UnsafeNumericCast<TARGET_TYPE>(lo + delta * d);
	}
};

template <>
interval_t CastInterpolation::Cast(const dtime_t &src, Vector &result);
template <>
double CastInterpolation::Interpolate(const double &lo, const double d, const double &hi);
template <>
dtime_t CastInterpolation::Interpolate(const dtime_t &lo, const double d, const dtime_t &hi);
template <>
timestamp_t CastInterpolation::Interpolate(const timestamp_t &lo, const double d, const timestamp_t &hi);
template <>
hugeint_t CastInterpolation::Interpolate(const hugeint_t &lo, const double d, const hugeint_t &hi);
template <>
interval_t CastInterpolation::Interpolate(const interval_t &lo, const double d, const interval_t &hi);
template <>
string_t CastInterpolation::Cast(const string_t &src, Vector &result);

// Continuous interpolation
template <bool DISCRETE>
struct Interpolator {
	Interpolator(const QuantileValue &q, const idx_t n_p, const bool desc_p)
	    : desc(desc_p), RN((double)(n_p - 1) * q.dbl), FRN(UnsafeNumericCast<idx_t>(floor(RN))),
	      CRN(UnsafeNumericCast<idx_t>(ceil(RN))), begin(0), end(n_p) {
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Interpolate(INPUT_TYPE lidx, INPUT_TYPE hidx, Vector &result, const ACCESSOR &accessor) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		if (lidx == hidx) {
			return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(lidx), result);
		} else {
			auto lo = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(lidx), result);
			auto hi = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(hidx), result);
			return CastInterpolation::Interpolate<TARGET_TYPE>(lo, RN - FRN, hi);
		}
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Operation(INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		QuantileCompare<ACCESSOR> comp(accessor, desc);
		if (CRN == FRN) {
			std::nth_element(v_t + begin, v_t + FRN, v_t + end, comp);
			return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
		} else {
			std::nth_element(v_t + begin, v_t + FRN, v_t + end, comp);
			std::nth_element(v_t + FRN, v_t + CRN, v_t + end, comp);
			auto lo = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[FRN]), result);
			auto hi = CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(v_t[CRN]), result);
			return CastInterpolation::Interpolate<TARGET_TYPE>(lo, RN - FRN, hi);
		}
	}

	template <class INPUT_TYPE, class TARGET_TYPE>
	inline TARGET_TYPE Extract(const INPUT_TYPE **dest, Vector &result) const {
		if (CRN == FRN) {
			return CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(*dest[0], result);
		} else {
			auto lo = CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(*dest[0], result);
			auto hi = CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(*dest[1], result);
			return CastInterpolation::Interpolate<TARGET_TYPE>(lo, RN - FRN, hi);
		}
	}

	const bool desc;
	const double RN;
	const idx_t FRN;
	const idx_t CRN;

	idx_t begin;
	idx_t end;
};

// Discrete "interpolation"
template <>
struct Interpolator<true> {
	static inline idx_t Index(const QuantileValue &q, const idx_t n) {
		idx_t floored;
		switch (q.val.type().id()) {
		case LogicalTypeId::DECIMAL: {
			//	Integer arithmetic for accuracy
			const auto integral = q.integral;
			const auto scaling = q.scaling;
			const auto scaled_q =
			    DecimalMultiplyOverflowCheck::Operation<hugeint_t, hugeint_t, hugeint_t>(Hugeint::Convert(n), integral);
			const auto scaled_n =
			    DecimalMultiplyOverflowCheck::Operation<hugeint_t, hugeint_t, hugeint_t>(Hugeint::Convert(n), scaling);
			floored = Cast::Operation<hugeint_t, idx_t>((scaled_n - scaled_q) / scaling);
			break;
		}
		default:
			const auto scaled_q = (double)(n * q.dbl);
			floored = UnsafeNumericCast<idx_t>(floor(n - scaled_q));
			break;
		}

		return MaxValue<idx_t>(1, n - floored) - 1;
	}

	Interpolator(const QuantileValue &q, const idx_t n_p, bool desc_p)
	    : desc(desc_p), FRN(Index(q, n_p)), CRN(FRN), begin(0), end(n_p) {
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Interpolate(INPUT_TYPE lidx, INPUT_TYPE hidx, Vector &result, const ACCESSOR &accessor) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(accessor(lidx), result);
	}

	template <class INPUT_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	typename ACCESSOR::RESULT_TYPE InterpolateInternal(INPUT_TYPE *v_t, const ACCESSOR &accessor = ACCESSOR()) const {
		QuantileCompare<ACCESSOR> comp(accessor, desc);
		std::nth_element(v_t + begin, v_t + FRN, v_t + end, comp);
		return accessor(v_t[FRN]);
	}

	template <class INPUT_TYPE, class TARGET_TYPE, typename ACCESSOR = QuantileDirect<INPUT_TYPE>>
	TARGET_TYPE Operation(INPUT_TYPE *v_t, Vector &result, const ACCESSOR &accessor = ACCESSOR()) const {
		using ACCESS_TYPE = typename ACCESSOR::RESULT_TYPE;
		return CastInterpolation::Cast<ACCESS_TYPE, TARGET_TYPE>(InterpolateInternal(v_t, accessor), result);
	}

	template <class INPUT_TYPE, class TARGET_TYPE>
	TARGET_TYPE Extract(const INPUT_TYPE **dest, Vector &result) const {
		return CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(*dest[0], result);
	}

	const bool desc;
	const idx_t FRN;
	const idx_t CRN;

	idx_t begin;
	idx_t end;
};

struct QuantileIncluded {
	inline explicit QuantileIncluded(const ValidityMask &fmask_p, const ValidityMask &dmask_p)
	    : fmask(fmask_p), dmask(dmask_p) {
	}

	inline bool operator()(const idx_t &idx) const {
		return fmask.RowIsValid(idx) && dmask.RowIsValid(idx);
	}

	inline bool AllValid() const {
		return fmask.AllValid() && dmask.AllValid();
	}

	const ValidityMask &fmask;
	const ValidityMask &dmask;
};

template <typename IDX>
struct QuantileSortTree : public MergeSortTree<IDX, IDX> {

	using BaseTree = MergeSortTree<IDX, IDX>;
	using Elements = typename BaseTree::Elements;

	explicit QuantileSortTree(Elements &&lowest_level) : BaseTree(std::move(lowest_level)) {
	}

	template <class INPUT_TYPE>
	static unique_ptr<QuantileSortTree> WindowInit(const INPUT_TYPE *data, AggregateInputData &aggr_input_data,
	                                               const ValidityMask &data_mask, const ValidityMask &filter_mask,
	                                               idx_t count) {
		//	Build the indirection array
		using ElementType = typename QuantileSortTree::ElementType;
		vector<ElementType> sorted(count);
		if (filter_mask.AllValid() && data_mask.AllValid()) {
			std::iota(sorted.begin(), sorted.end(), 0);
		} else {
			size_t valid = 0;
			QuantileIncluded included(filter_mask, data_mask);
			for (ElementType i = 0; i < count; ++i) {
				if (included(i)) {
					sorted[valid++] = i;
				}
			}
			sorted.resize(valid);
		}

		//	Sort it
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();
		using Accessor = QuantileIndirect<INPUT_TYPE>;
		Accessor indirect(data);
		QuantileCompare<Accessor> cmp(indirect, bind_data.desc);
		std::sort(sorted.begin(), sorted.end(), cmp);

		return make_uniq<QuantileSortTree>(std::move(sorted));
	}

	inline IDX SelectNth(const SubFrames &frames, size_t n) const {
		return BaseTree::NthElement(BaseTree::SelectNth(frames, n));
	}

	template <typename INPUT_TYPE, typename RESULT_TYPE, bool DISCRETE>
	RESULT_TYPE WindowScalar(const INPUT_TYPE *data, const SubFrames &frames, const idx_t n, Vector &result,
	                         const QuantileValue &q) const {
		D_ASSERT(n > 0);

		//	Find the interpolated indicies within the frame
		Interpolator<DISCRETE> interp(q, n, false);
		const auto lo_data = SelectNth(frames, interp.FRN);
		auto hi_data = lo_data;
		if (interp.CRN != interp.FRN) {
			hi_data = SelectNth(frames, interp.CRN);
		}

		//	Interpolate indirectly
		using ID = QuantileIndirect<INPUT_TYPE>;
		ID indirect(data);
		return interp.template Interpolate<idx_t, RESULT_TYPE, ID>(lo_data, hi_data, result, indirect);
	}

	template <typename INPUT_TYPE, typename CHILD_TYPE, bool DISCRETE>
	void WindowList(const INPUT_TYPE *data, const SubFrames &frames, const idx_t n, Vector &list, const idx_t lidx,
	                const QuantileBindData &bind_data) const {
		D_ASSERT(n > 0);

		// Result is a constant LIST<CHILD_TYPE> with a fixed length
		auto ldata = FlatVector::GetData<list_entry_t>(list);
		auto &lentry = ldata[lidx];
		lentry.offset = ListVector::GetListSize(list);
		lentry.length = bind_data.quantiles.size();

		ListVector::Reserve(list, lentry.offset + lentry.length);
		ListVector::SetListSize(list, lentry.offset + lentry.length);
		auto &result = ListVector::GetEntry(list);
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		using ID = QuantileIndirect<INPUT_TYPE>;
		ID indirect(data);
		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			Interpolator<DISCRETE> interp(quantile, n, false);

			const auto lo_data = SelectNth(frames, interp.FRN);
			auto hi_data = lo_data;
			if (interp.CRN != interp.FRN) {
				hi_data = SelectNth(frames, interp.CRN);
			}

			//	Interpolate indirectly
			rdata[lentry.offset + q] =
			    interp.template Interpolate<idx_t, CHILD_TYPE, ID>(lo_data, hi_data, result, indirect);
		}
	}
};

} // namespace duckdb
