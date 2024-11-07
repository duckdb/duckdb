//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/quantile_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "core_functions/aggregate/quantile_sort_tree.hpp"
#include "SkipList.h"

namespace duckdb {

struct QuantileOperation {
	template <class STATE>
	static void Initialize(STATE &state) {
		new (&state) STATE();
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void ConstantOperation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &unary_input,
	                              idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			Operation<INPUT_TYPE, STATE, OP>(state, input, unary_input);
		}
	}

	template <class INPUT_TYPE, class STATE, class OP>
	static void Operation(STATE &state, const INPUT_TYPE &input, AggregateUnaryInput &aggr_input) {
		state.AddElement(input, aggr_input.input);
	}

	template <class STATE, class OP>
	static void Combine(const STATE &source, STATE &target, AggregateInputData &) {
		if (source.v.empty()) {
			return;
		}
		target.v.insert(target.v.end(), source.v.begin(), source.v.end());
	}

	template <class STATE>
	static void Destroy(STATE &state, AggregateInputData &) {
		state.~STATE();
	}

	static bool IgnoreNull() {
		return true;
	}

	template <class STATE, class INPUT_TYPE>
	static void WindowInit(AggregateInputData &aggr_input_data, const WindowPartitionInput &partition,
	                       data_ptr_t g_state) {
		D_ASSERT(partition.inputs);

		const auto count = partition.count;
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
		if (count < std::numeric_limits<uint32_t>::max()) {
			window_state.qst32 = QuantileSortTree<uint32_t>::WindowInit<INPUT_TYPE>(aggr_input_data, partition);
		} else {
			window_state.qst64 = QuantileSortTree<uint64_t>::WindowInit<INPUT_TYPE>(aggr_input_data, partition);
		}
	}

	template <class INPUT_TYPE>
	static idx_t FrameSize(QuantileIncluded<INPUT_TYPE> &included, const SubFrames &frames) {
		//	Count the number of valid values
		idx_t n = 0;
		if (included.AllValid()) {
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

template <class T>
struct SkipLess {
	inline bool operator()(const T &lhi, const T &rhi) const {
		return lhi.second < rhi.second;
	}
};

template <typename INPUT_TYPE>
struct WindowQuantileState {
	// Windowed Quantile merge sort trees
	using QuantileSortTree32 = QuantileSortTree<uint32_t>;
	using QuantileSortTree64 = QuantileSortTree<uint64_t>;
	unique_ptr<QuantileSortTree32> qst32;
	unique_ptr<QuantileSortTree64> qst64;

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

	bool HasTrees() const {
		return qst32 || qst64;
	}

	template <typename RESULT_TYPE, bool DISCRETE>
	RESULT_TYPE WindowScalar(CursorType &data, const SubFrames &frames, const idx_t n, Vector &result,
	                         const QuantileValue &q) const {
		D_ASSERT(n > 0);
		if (qst32) {
			return qst32->WindowScalar<INPUT_TYPE, RESULT_TYPE, DISCRETE>(data, frames, n, result, q);
		} else if (qst64) {
			return qst64->WindowScalar<INPUT_TYPE, RESULT_TYPE, DISCRETE>(data, frames, n, result, q);
		} else if (s) {
			// Find the position(s) needed
			try {
				Interpolator<DISCRETE> interp(q, s->size(), false);
				s->at(interp.FRN, interp.CRN - interp.FRN + 1, skips);
				array<INPUT_TYPE, 2> dest;
				dest[0] = skips[0].second;
				if (skips.size() > 1) {
					dest[1] = skips[1].second;
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
		auto ldata = FlatVector::GetData<list_entry_t>(list);
		auto &lentry = ldata[lidx];
		lentry.offset = ListVector::GetListSize(list);
		lentry.length = bind_data.quantiles.size();

		ListVector::Reserve(list, lentry.offset + lentry.length);
		ListVector::SetListSize(list, lentry.offset + lentry.length);
		auto &result = ListVector::GetEntry(list);
		auto rdata = FlatVector::GetData<CHILD_TYPE>(result);

		for (const auto &q : bind_data.order) {
			const auto &quantile = bind_data.quantiles[q];
			rdata[lentry.offset + q] = WindowScalar<CHILD_TYPE, DISCRETE>(data, frames, n, result, quantile);
		}
	}
};

struct QuantileStandardType {
	template <class T>
	static T Operation(T input, AggregateInputData &) {
		return input;
	}
};

struct QuantileStringType {
	template <class T>
	static T Operation(T input, AggregateInputData &input_data) {
		if (input.IsInlined()) {
			return input;
		}
		auto string_data = input_data.allocator.Allocate(input.GetSize());
		memcpy(string_data, input.GetData(), input.GetSize());
		return string_t(char_ptr_cast(string_data), UnsafeNumericCast<uint32_t>(input.GetSize()));
	}
};

template <typename INPUT_TYPE, class TYPE_OP>
struct QuantileState {
	using InputType = INPUT_TYPE;
	using CursorType = QuantileCursor<INPUT_TYPE>;

	// Regular aggregation
	vector<INPUT_TYPE> v;

	// Window Quantile State
	unique_ptr<WindowQuantileState<INPUT_TYPE>> window_state;
	unique_ptr<CursorType> window_cursor;

	void AddElement(INPUT_TYPE element, AggregateInputData &aggr_input) {
		v.emplace_back(TYPE_OP::Operation(element, aggr_input));
	}

	bool HasTrees() const {
		return window_state && window_state->HasTrees();
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
		if (!window_state) {
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
