//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/quantile_sort_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/types/row/row_layout.hpp"
#include "core_functions/aggregate/quantile_helpers.hpp"
#include "duckdb/execution/merge_sort_tree.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include <algorithm>
#include <numeric>
#include <stdlib.h>
#include <utility>

namespace duckdb {

// Paged access
template <typename INPUT_TYPE>
struct QuantileCursor {
	explicit QuantileCursor(const WindowPartitionInput &partition) : inputs(*partition.inputs) {
		D_ASSERT(partition.column_ids.size() == 1);
		inputs.InitializeScan(scan, partition.column_ids);
		inputs.InitializeScanChunk(scan, page);

		D_ASSERT(partition.all_valid.size() == 1);
		all_valid = partition.all_valid[0];
	}

	inline sel_t RowOffset(idx_t row_idx) const {
		D_ASSERT(RowIsVisible(row_idx));
		return UnsafeNumericCast<sel_t>(row_idx - scan.current_row_index);
	}

	inline bool RowIsVisible(idx_t row_idx) const {
		return (row_idx < scan.next_row_index && scan.current_row_index <= row_idx);
	}

	inline idx_t Seek(idx_t row_idx) {
		if (!RowIsVisible(row_idx)) {
			inputs.Seek(row_idx, scan, page);
			data = FlatVector::GetData<INPUT_TYPE>(page.data[0]);
			validity = &FlatVector::Validity(page.data[0]);
		}
		return RowOffset(row_idx);
	}

	inline const INPUT_TYPE &operator[](idx_t row_idx) {
		const auto offset = Seek(row_idx);
		return data[offset];
	}

	inline bool RowIsValid(idx_t row_idx) {
		const auto offset = Seek(row_idx);
		return validity->RowIsValid(offset);
	}

	inline bool AllValid() {
		return all_valid;
	}

	//! Windowed paging
	const ColumnDataCollection &inputs;
	//! The state used for reading the collection on this thread
	ColumnDataScanState scan;
	//! The data chunk paged into into
	DataChunk page;
	//! The data pointer
	const INPUT_TYPE *data = nullptr;
	//! The validity mask
	const ValidityMask *validity = nullptr;
	//! Paged chunks do not track this but it is really necessary for performance
	bool all_valid;
};

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
	using CURSOR = QuantileCursor<RESULT_TYPE>;
	CURSOR &data;

	explicit QuantileIndirect(CURSOR &data_p) : data(data_p) {
	}

	inline RESULT_TYPE operator()(const INPUT_TYPE &input) const {
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
	const ACCESSOR &accessor_l;
	const ACCESSOR &accessor_r;
	const bool desc;

	// Single cursor for linear operations
	explicit QuantileCompare(const ACCESSOR &accessor, bool desc_p)
	    : accessor_l(accessor), accessor_r(accessor), desc(desc_p) {
	}

	// Independent cursors for sorting
	explicit QuantileCompare(const ACCESSOR &accessor_l, const ACCESSOR &accessor_r, bool desc_p)
	    : accessor_l(accessor_l), accessor_r(accessor_r), desc(desc_p) {
	}

	inline bool operator()(const INPUT_TYPE &lhs, const INPUT_TYPE &rhs) const {
		const auto lval = accessor_l(lhs);
		const auto rval = accessor_r(rhs);

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
		return LossyNumericCast<TARGET_TYPE>(lo + delta * d);
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
	    : desc(desc_p), RN((double)(n_p - 1) * q.dbl), FRN(ExactNumericCast<idx_t>(floor(RN))),
	      CRN(ExactNumericCast<idx_t>(ceil(RN))), begin(0), end(n_p) {
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
	inline TARGET_TYPE Extract(const INPUT_TYPE *dest, Vector &result) const {
		if (CRN == FRN) {
			return CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(dest[0], result);
		} else {
			auto lo = CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(dest[0], result);
			auto hi = CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(dest[1], result);
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
			const auto scaled_q = double(n) * q.dbl;
			floored = LossyNumericCast<idx_t>(floor(double(n) - scaled_q));
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
	TARGET_TYPE Extract(const INPUT_TYPE *dest, Vector &result) const {
		return CastInterpolation::Cast<INPUT_TYPE, TARGET_TYPE>(dest[0], result);
	}

	const bool desc;
	const idx_t FRN;
	const idx_t CRN;

	idx_t begin;
	idx_t end;
};

template <typename INPUT_TYPE>
struct QuantileIncluded {
	using CURSOR_TYPE = QuantileCursor<INPUT_TYPE>;

	inline explicit QuantileIncluded(const ValidityMask &fmask_p, CURSOR_TYPE &dmask_p)
	    : fmask(fmask_p), dmask(dmask_p) {
	}

	inline bool operator()(const idx_t &idx) {
		return fmask.RowIsValid(idx) && dmask.RowIsValid(idx);
	}

	inline bool AllValid() {
		return fmask.AllValid() && dmask.AllValid();
	}

	const ValidityMask &fmask;
	CURSOR_TYPE &dmask;
};

// Shared untemplated sort logic
static unique_ptr<GlobalSortState> SortQuantileIndices(const WindowPartitionInput &partition, // NOLINT
                                                       const LogicalType &index_type, OrderType order_type) {
	auto &inputs = *partition.inputs;
	const auto &filter_mask = partition.filter_mask;

	// Sort the unfiltered indices by the argument values
	vector<LogicalType> payload_types;
	payload_types.emplace_back(index_type);

	idx_t capacity = STANDARD_VECTOR_SIZE;
	DataChunk payload;
	payload.Initialize(inputs.GetAllocator(), payload_types, capacity);
	RowLayout payload_layout;
	payload_layout.Initialize(payload.GetTypes());
	SelectionVector filtered(capacity);

	// TODO: Two pass parallel sorting using Build
	ColumnDataScanState state;
	DataChunk sort;
	inputs.InitializeScan(state, partition.column_ids);
	inputs.InitializeScanChunk(state, sort);
	auto order_expr = make_uniq<BoundConstantExpression>(Value(sort.GetTypes()[0]));
	vector<BoundOrderByNode> orders;
	orders.emplace_back(BoundOrderByNode(order_type, OrderByNullType::NULLS_LAST, std::move(order_expr)));

	auto &buffer_manager = BufferManager::GetBufferManager(partition.context);
	auto global_sort = make_uniq<GlobalSortState>(buffer_manager, orders, payload_layout);
	global_sort->external = ClientConfig::GetConfig(partition.context).force_external;
	const auto memory_per_thread = PhysicalOperator::GetMaxThreadMemory(partition.context);

	LocalSortState local_sort;
	local_sort.Initialize(*global_sort, global_sort->buffer_manager);

	//	Build the indirection array by scanning the valid indices
	while (inputs.Scan(state, sort)) {
		// Match the payload to the scanned data
		if (sort.size() > capacity) {
			payload.Destroy();
			capacity = sort.size();
			payload.Initialize(inputs.GetAllocator(), payload_types, capacity);
			filtered.Initialize(capacity);
		} else {
			payload.Reset();
		}
		auto &indices = payload.data[0];
		payload.SetCardinality(sort);
		indices.Sequence(int64_t(state.current_row_index), 1, payload.size());

		if (!filter_mask.AllValid() || !partition.all_valid[0]) {
			auto &key = sort.data[0];
			auto &validity = FlatVector::Validity(key);
			idx_t valid = 0;
			for (sel_t i = 0; i < sort.size(); ++i) {
				if (filter_mask.RowIsValid(i + state.current_row_index) && validity.RowIsValid(i)) {
					filtered[valid++] = i;
				}
			}
			if (valid < sort.size()) {
				payload.Slice(filtered, valid);
				sort.Slice(filtered, valid);
			}
		}
		local_sort.SinkChunk(sort, payload);
		if (local_sort.SizeInBytes() > memory_per_thread) {
			local_sort.Sort(*global_sort, true);
		}
	}
	global_sort->AddLocalState(local_sort);

	//	Sort it
	global_sort->PrepareMergePhase();
	while (global_sort->sorted_blocks.size() > 1) {
		global_sort->InitializeMergeRound();
		MergeSorter merge_sorter(*global_sort, global_sort->buffer_manager);
		merge_sorter.PerformInMergeRound();
		global_sort->CompleteMergeRound(false);
	}

	return global_sort;
}

template <typename IDX>
struct QuantileSortTree : public MergeSortTree<IDX, IDX> {

	using BaseTree = MergeSortTree<IDX, IDX>;
	using Elements = typename BaseTree::Elements;

	explicit QuantileSortTree(Elements &&lowest_level) {
		BaseTree::Allocate(lowest_level.size());
		BaseTree::LowestLevel() = std::move(lowest_level);
	}

	template <class INPUT_TYPE>
	static unique_ptr<QuantileSortTree> WindowInit(AggregateInputData &aggr_input_data,
	                                               const WindowPartitionInput &partition) {
		auto &inputs = *partition.inputs;

		// Sort the unfiltered indices by the argument values
		using ElementType = typename QuantileSortTree::ElementType;
		vector<LogicalType> payload_types;
		switch (sizeof(ElementType)) {
		case sizeof(int64_t):
			payload_types.emplace_back(LogicalType::BIGINT);
			break;
		case sizeof(int32_t):
			payload_types.emplace_back(LogicalType::INTEGER);
			break;
		default:
			throw InternalException("Unsupported Quantile Sort Tree index size");
		}

		// TODO: Two pass parallel sorting using Build
		auto &bind_data = aggr_input_data.bind_data->Cast<QuantileBindData>();
		auto order_type = bind_data.desc ? OrderType::DESCENDING : OrderType::ASCENDING;
		auto global_sort = SortQuantileIndices(partition, payload_types[0], order_type);

		// Now scan the sorted indices into an array we can use as the leaves
		vector<ElementType> sorted;
		if (!global_sort->sorted_blocks.empty()) {
			PayloadScanner scanner(*global_sort);
			DataChunk payload;
			payload.Initialize(inputs.GetAllocator(), payload_types);
			sorted.resize(scanner.Remaining());
			for (;;) {
				idx_t row_idx = scanner.Scanned();
				scanner.Scan(payload);
				if (payload.size() == 0) {
					break;
				}
				auto &indices = payload.data[0];
				auto data = FlatVector::GetData<ElementType>(indices);

				std::copy(data, data + payload.size(), sorted.data() + row_idx);
			}
		}

		return make_uniq<QuantileSortTree>(std::move(sorted));
	}

	inline IDX SelectNth(const SubFrames &frames, size_t n) const {
		return BaseTree::NthElement(BaseTree::SelectNth(frames, n));
	}

	template <typename INPUT_TYPE, typename RESULT_TYPE, bool DISCRETE>
	RESULT_TYPE WindowScalar(QuantileCursor<INPUT_TYPE> &data, const SubFrames &frames, const idx_t n, Vector &result,
	                         const QuantileValue &q) {
		D_ASSERT(n > 0);

		//	Thread safe and idempotent.
		BaseTree::Build();

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
	void WindowList(QuantileCursor<INPUT_TYPE> &data, const SubFrames &frames, const idx_t n, Vector &list,
	                const idx_t lidx, const QuantileBindData &bind_data) {
		D_ASSERT(n > 0);

		//	Thread safe and idempotent.
		BaseTree::Build();

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
