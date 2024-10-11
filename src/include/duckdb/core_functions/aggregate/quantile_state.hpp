//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/core_functions/aggregate/quantile_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/core_functions/aggregate/quantile_sort_tree.hpp"
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
		D_ASSERT(partition.input_count == 1);

		auto inputs = partition.inputs;
		const auto count = partition.count;
		const auto &filter_mask = partition.filter_mask;
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

		const auto data = FlatVector::GetData<const INPUT_TYPE>(inputs[0]);
		const auto &data_mask = FlatVector::Validity(inputs[0]);

		//	Build the tree
		auto &state = *reinterpret_cast<STATE *>(g_state);
		auto &window_state = state.GetOrCreateWindowState();
		if (count < std::numeric_limits<uint32_t>::max()) {
			window_state.qst32 = QuantileSortTree<uint32_t>::WindowInit<INPUT_TYPE>(data, aggr_input_data, data_mask,
			                                                                        filter_mask, count);
		} else {
			window_state.qst64 = QuantileSortTree<uint64_t>::WindowInit<INPUT_TYPE>(data, aggr_input_data, data_mask,
			                                                                        filter_mask, count);
		}
	}

	static idx_t FrameSize(const QuantileIncluded &included, const SubFrames &frames) {
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
struct PointerLess {
	inline bool operator()(const T &lhi, const T &rhi) const {
		return *lhi < *rhi;
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
	using PointerType = const INPUT_TYPE *;
	using SkipListType = duckdb_skiplistlib::skip_list::HeadNode<PointerType, PointerLess<PointerType>>;
	SubFrames prevs;
	unique_ptr<SkipListType> s;
	mutable vector<PointerType> dest;

	// Windowed MAD indirection
	idx_t count;
	vector<idx_t> m;

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
		const INPUT_TYPE *data;
		const QuantileIncluded &included;

		inline SkipListUpdater(SkipListType &skip, const INPUT_TYPE *data, const QuantileIncluded &included)
		    : skip(skip), data(data), included(included) {
		}

		inline void Neither(idx_t begin, idx_t end) {
		}

		inline void Left(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					skip.remove(data + begin);
				}
			}
		}

		inline void Right(idx_t begin, idx_t end) {
			for (; begin < end; ++begin) {
				if (included(begin)) {
					skip.insert(data + begin);
				}
			}
		}

		inline void Both(idx_t begin, idx_t end) {
		}
	};

	void UpdateSkip(const INPUT_TYPE *data, const SubFrames &frames, const QuantileIncluded &included) {
		//	No overlap, or no data
		if (!s || prevs.back().end <= frames.front().start || frames.back().end <= prevs.front().start) {
			auto &skip = GetSkipList(true);
			for (const auto &frame : frames) {
				for (auto i = frame.start; i < frame.end; ++i) {
					if (included(i)) {
						skip.insert(data + i);
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
	RESULT_TYPE WindowScalar(const INPUT_TYPE *data, const SubFrames &frames, const idx_t n, Vector &result,
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
				s->at(interp.FRN, interp.CRN - interp.FRN + 1, dest);
				return interp.template Extract<INPUT_TYPE, RESULT_TYPE>(dest.data(), result);
			} catch (const duckdb_skiplistlib::skip_list::IndexError &idx_err) {
				throw InternalException(idx_err.message());
			}
		} else {
			throw InternalException("No accelerator for scalar QUANTILE");
		}
	}

	template <typename CHILD_TYPE, bool DISCRETE>
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

	// Regular aggregation
	vector<INPUT_TYPE> v;

	// Window Quantile State
	unique_ptr<WindowQuantileState<INPUT_TYPE>> window_state;

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
};

} // namespace duckdb
