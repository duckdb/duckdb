//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/quantile_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "core_functions/aggregate/quantile_sort_tree.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/list_segment.hpp"
#include "duckdb/function/aggregate/list_aggregate.hpp"
#include "SkipList.h"

namespace duckdb {

//! Flattens the values of a linked list into a contiguous array for interpolation.
//! The flattened values are mutable - the interpolators partially sort them in place.
//! The flattened chunk is cached in the finalize data's local state, so that it is allocated (at most) once per
//! result chunk instead of once per finalized group.
template <class INPUT_TYPE>
struct FlattenedQuantileValues : FunctionLocalState {
	FlattenedQuantileValues(const LogicalType &type, idx_t capacity_p) : capacity(capacity_p) {
		chunk.Initialize(Allocator::DefaultAllocator(), {type}, capacity_p);
	}

	//! Flatten the values of the given linked list into the chunk cached in the finalize data
	static FlattenedQuantileValues &Flatten(AggregateFinalizeData &finalize_data, const LinkedList &linked_list) {
		const auto type = PrimitiveToLogicalType<INPUT_TYPE>();
		const auto required_capacity = MaxValue<idx_t>(linked_list.total_capacity, 1);
		if (!finalize_data.local_state) {
			finalize_data.local_state = make_uniq<FlattenedQuantileValues>(type, NextPowerOfTwo(required_capacity));
		}
		auto &values = finalize_data.local_state->Cast<FlattenedQuantileValues>();
		if (values.capacity < required_capacity) {
			// grow the cached chunk
			values.capacity = NextPowerOfTwo(required_capacity);
			values.chunk.Destroy();
			values.chunk.Initialize(Allocator::DefaultAllocator(), {type}, values.capacity);
		} else {
			// re-use the allocated chunk - resetting clears the string heap of the previously flattened state
			values.chunk.Reset();
		}
		ListSegmentFunctions functions;
		GetSegmentDataFunctions(functions, type);
		functions.BuildListVector(linked_list, values.chunk.data[0], 0);
		return values;
	}

	INPUT_TYPE *Data() {
		return FlatVector::GetDataMutable<INPUT_TYPE>(chunk.data[0]);
	}

	//! The flattened values (a single-column chunk) - strings are materialized into the chunk's string heap
	DataChunk chunk;
	//! The allocated capacity of the chunk
	idx_t capacity;
};

struct QuantileOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	//! The type of the values buffered in the linked list - used by the shared list update/combine functions
	static LogicalType GetElementType(AggregateInputData &aggr_input_data) {
		return aggr_input_data.function.GetArguments()[0];
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		state.~STATE();
	}

	template <class STATE, class INPUT_TYPE>
	static void WindowInit(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                       data_ptr_t g_state) {
		D_ASSERT(partition.inputs);

		const auto &stats = partition.stats;

		//	If frames overlap significantly, then use local skip lists.
		if (stats[0].end <= stats[1].begin) {
			//	Frames can overlap
			const auto overlap = double(stats[1].begin - stats[0].end);
			const auto cover = double(stats[1].end - stats[0].begin);
			const auto ratio = overlap / cover;
			if (ratio > .75) {
				return;
			}
		}

		//	Build the tree
		auto &state = *reinterpret_cast<STATE *>(g_state);
		auto &window_state = state.GetOrCreateWindowState();
		window_state.qst = make_uniq<QuantileSortTree>(aggr_input_data, partition);
	}

	template <class INPUT_TYPE>
	static idx_t FrameSize(QuantileIncluded<INPUT_TYPE> &included, const SubFrames &frames) {
		//	Count the number of valid values
		idx_t n = 0;
		if (included.CannotHaveNull()) {
			for (const auto &frame : frames) {
				n += frame.end - frame.start;
			}
		} else {
			//	NULLs or FILTERed values,
			for (const auto &frame : frames) {
				for (auto i = frame.start; i < frame.end; ++i) {
					n += included(i);
				}
			}
		}

		return n;
	}
};

//! Creates a quantile aggregate that buffers the input values in a linked list, sharing the "list" aggregate
//! callbacks for update/combine - the operation only provides the finalizer.
//! Quantiles ignore NULL values, so they are filtered out while appending.
template <class STATE, class RESULT_TYPE, class OP>
AggregateFunction QuantileBufferingAggregate(const LogicalType &input_type, const LogicalType &result_type) {
	return AggregateFunction({input_type}, result_type, AggregateFunction::StateSize<STATE>,
	                         AggregateFunction::StateInitialize<STATE, OP, AggregateDestructorType::LEGACY>,
	                         ListUpdateFunction<true>, ListCombineFunction<OP>,
	                         AggregateFunction::StateFinalize<STATE, RESULT_TYPE, OP>,
	                         FunctionNullHandling::DEFAULT_NULL_HANDLING, AggregateFunction::NoClusterUpdate(),
	                         AggregateFunction::NoBind(), AggregateFunction::StateDestroy<STATE, OP>);
}

template <class T>
struct SkipLess {
	inline bool operator()(const T &lhi, const T &rhi) const {
		return lhi.second < rhi.second;
	}
};

template <typename INPUT_TYPE>
struct WindowQuantileState {
	// Windowed Quantile merge sort trees
	unique_ptr<QuantileSortTree> qst;

	// Windowed Quantile skip lists
	using SkipType = pair<idx_t, INPUT_TYPE>;
	using SkipListType = duckdb_skiplistlib::skip_list::HeadNode<SkipType, SkipLess<SkipType>>;
	SubFrames prevs;
	unique_ptr<SkipListType> s;
	mutable vector<SkipType> skips;

	// Windowed MAD indirection
	idx_t count;
	vector<idx_t> m;

	using IncludedType = QuantileIncluded<INPUT_TYPE>;
	using CursorType = QuantileCursor<INPUT_TYPE>;

	WindowQuantileState() : count(0) {
	}

	inline void SetCount(size_t count_p) {
		count = count_p;
		if (count >= m.size()) {
			m.resize(count);
		}
	}

	inline SkipListType &GetSkipList(bool reset = false) {
		if (reset || !s) {
			s.reset();
			s = make_uniq<SkipListType>();
		}
		return *s;
	}

	struct SkipListUpdater {
		SkipListType &skip;
		CursorType &data;
		IncludedType &included;

		inline SkipListUpdater(SkipListType &skip, CursorType &data, IncludedType &included)
		    : skip(skip), data(data), included(included) {
		}

		inline void Neither(idx_t begin, idx_t end) {
		}

		inline void Left(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					skip.remove(SkipType(begin, data[begin]));
				}
			}
		}

		inline void Right(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					skip.insert(SkipType(begin, data[begin]));
				}
			}
		}

		inline void Both(idx_t begin, idx_t end) {
		}
	};

	void UpdateSkip(CursorType &data, const SubFrames &frames, IncludedType &included) {
		//	No overlap, or no data
		if (!s || prevs.back().end <= frames.front().start || frames.back().end <= prevs.front().start) {
			auto &skip = GetSkipList(true);
			for (const auto &frame : frames) {
				for (auto i = frame.start; i < frame.end; ++i) {
					if (included(i)) {
						skip.insert(SkipType(i, data[i]));
					}
				}
			}
		} else {
			auto &skip = GetSkipList();
			SkipListUpdater updater(skip, data, included);
			AggregateExecutor::IntersectFrames(prevs, frames, updater);
		}
	}

	bool HasTree() const {
		return qst.get();
	}

	template <typename RESULT_TYPE, bool DISCRETE>
	RESULT_TYPE WindowScalar(CursorType &data, const SubFrames &frames, const idx_t n, Vector &result,
	                         const QuantileValue &q) const {
		D_ASSERT(n > 0);
		if (qst) {
			return qst->WindowScalar<INPUT_TYPE, RESULT_TYPE, DISCRETE>(data, frames, n, result, q);
		} else if (s) {
			// Find the position(s) needed
			try {
				QuantileInterpolator<DISCRETE> interp(q, s->size(), false);
				s->at(interp.FRN, interp.CRN - interp.FRN + 1, skips);
				array<INPUT_TYPE, 2> dest;
				dest[0] = skips[0].second;
				if (skips.size() > 1) {
					dest[1] = skips[1].second;
				} else {
					// Avoid UMA
					dest[1] = skips[0].second;
				}
				return interp.template Extract<INPUT_TYPE, RESULT_TYPE>(dest.data(), result);
			} catch (const duckdb_skiplistlib::skip_list::IndexError &idx_err) {
				throw InternalException(idx_err.message());
			}
		} else {
			throw InternalException("No accelerator for scalar QUANTILE");
		}
	}

	template <typename CHILD_TYPE, bool DISCRETE>
	void WindowList(CursorType &data, const SubFrames &frames, const idx_t n, Vector &list, const idx_t lidx,
	                const QuantileBindData &bind_data) const {
		D_ASSERT(n > 0);
		// Result is a constant LIST<CHILD_TYPE> with a fixed length
		auto ldata = FlatVector::GetDataMutable<list_entry_t>(list);
		auto &lentry = ldata[lidx];
		lentry.offset = ListVector::GetListSize(list);
		lentry.length = bind_data.quantiles.size();

		ListVector::Reserve(list, lentry.offset + lentry.length);
		ListVector::SetListSize(list, lentry.offset + lentry.length);
		auto &result = ListVector::GetChildMutable(list);
		auto rdata = FlatVector::GetDataMutable<CHILD_TYPE>(result);

		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			rdata[lentry.offset + q] = WindowScalar<CHILD_TYPE, DISCRETE>(data, frames, n, result, quantile);
		}
	}
};

//! Regular aggregation buffers the values in the linked list of the ListAggState base,
//! shared with the "list" aggregate callbacks
template <typename INPUT_TYPE>
struct QuantileState : ListAggState {
	using InputType = INPUT_TYPE;
	using CursorType = QuantileCursor<INPUT_TYPE>;

	// Window Quantile State (only used for window execution)
	unique_ptr<WindowQuantileState<INPUT_TYPE>> window_state;
	unique_ptr<CursorType> window_cursor;

	bool HasTree() const {
		return window_state && window_state->HasTree();
	}
	WindowQuantileState<INPUT_TYPE> &GetOrCreateWindowState() {
		if (!window_state) {
			window_state = make_uniq<WindowQuantileState<INPUT_TYPE>>();
		}
		return *window_state;
	}
	WindowQuantileState<INPUT_TYPE> &GetWindowState() {
		return *window_state;
	}
	const WindowQuantileState<INPUT_TYPE> &GetWindowState() const {
		return *window_state;
	}

	CursorType &GetOrCreateWindowCursor(const WindowPartitionInput &partition) {
		if (!window_cursor) {
			window_cursor = make_uniq<CursorType>(partition);
		}
		return *window_cursor;
	}
	CursorType &GetWindowCursor() {
		return *window_cursor;
	}
	const CursorType &GetWindowCursor() const {
		return *window_cursor;
	}
};

} // namespace duckdb
