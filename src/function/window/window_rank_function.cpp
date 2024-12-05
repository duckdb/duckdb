#include "duckdb/function/window/window_rank_function.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowPeerState
//===--------------------------------------------------------------------===//
//	Base class for non-aggregate functions that use peer boundaries
class WindowPeerState : public WindowExecutorBoundsState {
public:
	explicit WindowPeerState(const WindowExecutorGlobalState &gstate) : WindowExecutorBoundsState(gstate) {
	}

public:
	uint64_t dense_rank = 1;
	uint64_t rank_equal = 0;
	uint64_t rank = 1;

	void NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx);
};

void WindowPeerState::NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx) {
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
// WindowRankExecutor
//===--------------------------------------------------------------------===//
WindowRankExecutor::WindowRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                       WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState> WindowRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                          DataChunk &eval_chunk, Vector &result, idx_t count, idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);

	//	Reset to "previous" row
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = NumericCast<int64_t>(lpeer.rank);
	}
}

//===--------------------------------------------------------------------===//
// WindowDenseRankExecutor
//===--------------------------------------------------------------------===//
WindowDenseRankExecutor::WindowDenseRankExecutor(BoundWindowExpression &wexpr, ClientContext &context,
                                                 WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState>
WindowDenseRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowDenseRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                               DataChunk &eval_chunk, Vector &result, idx_t count,
                                               idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerState>();

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
    : WindowExecutor(wexpr, context, shared) {
}

unique_ptr<WindowExecutorLocalState>
WindowPercentRankExecutor::GetLocalState(const WindowExecutorGlobalState &gstate) const {
	return make_uniq<WindowPeerState>(gstate);
}

void WindowPercentRankExecutor::EvaluateInternal(WindowExecutorGlobalState &gstate, WindowExecutorLocalState &lstate,
                                                 DataChunk &eval_chunk, Vector &result, idx_t count,
                                                 idx_t row_idx) const {
	auto &lpeer = lstate.Cast<WindowPeerState>();
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_END]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<double>(result);

	//	Reset to "previous" row
	lpeer.rank = (peer_begin[0] - partition_begin[0]) + 1;
	lpeer.rank_equal = (row_idx - peer_begin[0]);

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		auto denom = static_cast<double>(NumericCast<int64_t>(partition_end[i] - partition_begin[i] - 1));
		double percent_rank = denom > 0 ? ((double)lpeer.rank - 1) / denom : 0;
		rdata[i] = percent_rank;
	}
}

} // namespace duckdb
