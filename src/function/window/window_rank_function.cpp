#include "duckdb/function/window/window_rank_function.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/function/window/window_token_tree.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowPeerGlobalState
//===--------------------------------------------------------------------===//
class WindowPeerGlobalState : public WindowExecutorGlobalState {
public:
	WindowPeerGlobalState(const WindowPeerExecutor &executor, const idx_t payload_count,
	                      const ValidityMask &partition_mask, const ValidityMask &order_mask)
	    : WindowExecutorGlobalState(executor, payload_count, partition_mask, order_mask) {
		if (!executor.arg_order_idx.empty()) {
			token_tree = make_uniq<WindowTokenTree>(executor.context, executor.wexpr.arg_orders, executor.arg_order_idx,
			                                        payload_count);
		}
	}

	//! The token tree for ORDER BY arguments
	unique_ptr<WindowTokenTree> token_tree;
};

//===--------------------------------------------------------------------===//
// WindowPeerLocalState
//===--------------------------------------------------------------------===//
//	Base class for non-aggregate functions that use peer boundaries
class WindowPeerLocalState : public WindowExecutorBoundsState {
public:
	explicit WindowPeerLocalState(const WindowPeerGlobalState &gpstate)
	    : WindowExecutorBoundsState(gpstate), gpstate(gpstate) {
		if (gpstate.token_tree) {
			local_tree = gpstate.token_tree->GetLocalState();
		}
	}

	//! Accumulate the secondary sort values
	void Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
	          idx_t input_idx) override;
	//! Finish the sinking and prepare to scan
	void Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) override;

	void NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx);

	uint64_t dense_rank = 1;
	uint64_t rank_equal = 0;
	uint64_t rank = 1;

	//! The corresponding global peer state
	const WindowPeerGlobalState &gpstate;
	//! The optional sorting state for secondary sorts
	unique_ptr<WindowAggregatorState> local_tree;
};

void WindowPeerLocalState::Sink(WindowExecutorGlobalState &gstate, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                idx_t input_idx) {
	WindowExecutorBoundsState::Sink(gstate, sink_chunk, coll_chunk, input_idx);

	if (local_tree) {
		auto &local_tokens = local_tree->Cast<WindowMergeSortTreeLocalState>();
		local_tokens.SinkChunk(sink_chunk, input_idx, nullptr, 0);
	}
}

void WindowPeerLocalState::Finalize(WindowExecutorGlobalState &gstate, CollectionPtr collection) {
	WindowExecutorBoundsState::Finalize(gstate, collection);

	if (local_tree) {
		auto &local_tokens = local_tree->Cast<WindowMergeSortTreeLocalState>();
		local_tokens.Sort();
		local_tokens.window_tree.Build();
	}
}

void WindowPeerLocalState::NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx) {
	if (partition_begin == row_idx) {
		dense_rank = 1;
		rank = 1;
		rank_equal = 0;
	} else if (peer_begin == row_idx) {
		dense_rank++;
		rank += rank_equal;
		rank_equal = 0;
	}
	rank_equal++;
}

//===--------------------------------------------------------------------===//
// WindowPeerExecutor
//===--------------------------------------------------------------------===//
WindowPeerExecutor::WindowPeerExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                       WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {

	for (const auto &order : wexpr.arg_orders) {
		arg_order_idx.emplace_back(shared.RegisterSink(order.expression));
	}
}

unique_ptr<WindowExecutorGlobalState> WindowPeerExecutor::GetGlobalState(const idx_t payload_count,
                                                                         const ValidityMask &partition_mask,
                                                                         const ValidityMask &order_mask) const {
	return make_uniq<WindowPeerGlobalState>(*this, payload_count, partition_mask, order_mask);
}

unique_ptr<WindowExecutorLocalState> WindowPeerExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerLocalState>(gstate.Cast<WindowPeerGlobalState>());
}

//===--------------------------------------------------------------------===//
// WindowRankExecutor
//===--------------------------------------------------------------------===//
WindowRankExecutor::WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                       WindowSharedExpressions &shared)
    : WindowPeerExecutor(wexpr, context, shared) {
}

void WindowRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                          DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &gpeer = gstate.Cast<WindowPeerGlobalState>();
	auto &lpeer = lstate.Cast<WindowPeerLocalState>();
	auto rdata = FlatVector::GetData<uint64_t>(result);

	if (gpeer.token_tree) {
		auto frame_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_BEGIN]);
		auto frame_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_END]);
		for (idx_t i = 0; i < count; ++i, ++row_idx) {
			rdata[i] = gpeer.token_tree->Rank(frame_begin[i], frame_end[i], row_idx);
		}
		return;
	}

	//	Reset to "previous" row
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = lpeer.rank;
	}
}

//===--------------------------------------------------------------------===//
// WindowDenseRankExecutor
//===--------------------------------------------------------------------===//
WindowDenseRankExecutor::WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowPeerExecutor(wexpr, context, shared) {
}

void WindowDenseRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerLocalState>();

	auto &order_mask = gstate.order_mask;
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);

	//	Reset to "previous" row
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	//	The previous dense rank is the number of order mask bits in [partition_begin, row_idx)
	lpeer.dense_rank = 0;

	auto order_begin = partition_begin[0];
	idx_t begin_idx;
	idx_t begin_offset;
	order_mask.GetEntryIndex(order_begin, begin_idx, begin_offset);

	auto order_end = row_idx;
	idx_t end_idx;
	idx_t end_offset;
	order_mask.GetEntryIndex(order_end, end_idx, end_offset);

	//	If they are in the same entry, just loop
	if (begin_idx == end_idx) {
		const auto entry = order_mask.GetValidityEntry(begin_idx);
		for (; begin_offset < end_offset; ++begin_offset) {
			lpeer.dense_rank += order_mask.RowIsValid(entry, begin_offset);
		}
	} else {
		// Count the ragged bits at the start of the partition
		if (begin_offset) {
			const auto entry = order_mask.GetValidityEntry(begin_idx);
			for (; begin_offset < order_mask.BITS_PER_VALUE; ++begin_offset) {
				lpeer.dense_rank += order_mask.RowIsValid(entry, begin_offset);
				++order_begin;
			}
			++begin_idx;
		}

		//	Count the the aligned bits.
		ValidityMask tail_mask(order_mask.GetData() + begin_idx, end_idx - begin_idx);
		lpeer.dense_rank += tail_mask.CountValid(order_end - order_begin);
	}

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = NumericCast<int64_t>(lpeer.dense_rank);
	}
}

//===--------------------------------------------------------------------===//
// WindowPercentRankExecutor
//===--------------------------------------------------------------------===//
WindowPercentRankExecutor::WindowPercentRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                     WindowSharedExpressions &shared)
    : WindowPeerExecutor(wexpr, context, shared) {
}

void WindowPercentRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                                 DataChunk &eval_chunk, Vector &result, idx_t count,
                                                 idx_t row_idx) const {
	auto &gpeer = gstate.Cast<WindowPeerGlobalState>();
	auto &lpeer = lstate.Cast<WindowPeerLocalState>();
	auto rdata = FlatVector::GetData<double>(result);

	if (gpeer.token_tree) {
		auto frame_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_BEGIN]);
		auto frame_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_END]);
		for (idx_t i = 0; i < count; ++i, ++row_idx) {
			auto denom = static_cast<double>(NumericCast<int64_t>(frame_end[i] - frame_begin[i] - 1));
			const auto rank = gpeer.token_tree->Rank(frame_begin[i], frame_end[i], row_idx);
			double percent_rank = denom > 0 ? ((double)rank - 1) / denom : 0;
			rdata[i] = percent_rank;
		}
		return;
	}

	//	Reset to "previous" row
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_END]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		auto denom = static_cast<double>(NumericCast<int64_t>(partition_end[i] - partition_begin[i] - 1));
		double percent_rank = denom > 0 ? ((double)lpeer.rank - 1) / denom : 0;
		rdata[i] = percent_rank;
	}
}

//===--------------------------------------------------------------------===//
// WindowCumeDistExecutor
//===--------------------------------------------------------------------===//
WindowCumeDistExecutor::WindowCumeDistExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                               WindowSharedExpressions &shared)
    : WindowPeerExecutor(wexpr, context, shared) {
}

void WindowCumeDistExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                              DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &gpeer = gstate.Cast<WindowPeerGlobalState>();
	auto &lpeer = lstate.Cast<WindowPeerLocalState>();
	auto rdata = FlatVector::GetData<double>(result);

	if (gpeer.token_tree) {
		auto frame_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_BEGIN]);
		auto frame_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_END]);
		for (idx_t i = 0; i < count; ++i, ++row_idx) {
			const auto denom = static_cast<double>(NumericCast<int64_t>(frame_end[i] - frame_begin[i]));
			const auto peer_end = gpeer.token_tree->PeerEnd(frame_begin[i], frame_end[i], row_idx);
			const auto num = static_cast<double>(peer_end - frame_begin[i]);
			rdata[i] = denom > 0 ? (num / denom) : 0;
		}
		return;
	}

	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_END]);
	auto peer_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_END]);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		const auto denom = static_cast<double>(NumericCast<int64_t>(partition_end[i] - partition_begin[i]));
		const auto num = static_cast<double>(peer_end[i] - partition_begin[i]);
		rdata[i] = denom > 0 ? (num / denom) : 0;
	}
}

} // namespace duckdb
