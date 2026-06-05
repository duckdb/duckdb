#include "duckdb/function/window/window_rank_function.hpp"
#include "duckdb/function/window/window_shared_expressions.hpp"
#include "duckdb/function/window/window_token_tree.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_window_expression.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// WindowPeerGlobalState
//===--------------------------------------------------------------------===//
class WindowPeerGlobalState : public WindowExecutorGlobalState {
public:
	WindowPeerGlobalState(ClientContext &client, const WindowPeerExecutor &executor, const idx_t payload_count,
	                      const ValidityMask &partition_mask, const ValidityMask &order_mask)
	    : WindowExecutorGlobalState(client, executor, payload_count, partition_mask, order_mask) {
		if (!executor.arg_order_idx.empty()) {
			use_framing = true;

			//	If the argument order is a prefix of the partition ordering
			//	(and the optimizer is enabled), then we can just use the partition ordering.
			auto &wexpr = executor.wexpr;
			auto &arg_orders = executor.wexpr.arg_orders;
			const auto optimize = ClientConfig::GetConfig(client).enable_optimizer;
			if (!optimize || BoundWindowExpression::GetSharedOrders(wexpr.orders, arg_orders) != arg_orders.size()) {
				token_tree = make_uniq<WindowTokenTree>(client, arg_orders, executor.arg_order_idx, payload_count);
			}
		}
	}

	//! Use framing instead of partitions (ORDER BY arguments)
	bool use_framing = false;

	//! The token tree for ORDER BY arguments
	unique_ptr<WindowTokenTree> token_tree;
};

//===--------------------------------------------------------------------===//
// WindowPeerLocalState
//===--------------------------------------------------------------------===//
//	Base class for non-aggregate functions that use peer boundaries
class WindowPeerLocalState : public WindowExecutorBoundsLocalState {
public:
	WindowPeerLocalState(ExecutionContext &context, const WindowPeerGlobalState &gpstate)
	    : WindowExecutorBoundsLocalState(context, gpstate), gpstate(gpstate) {
		if (gpstate.token_tree) {
			local_tree = gpstate.token_tree->GetLocalState(context);
		}
	}

	//! Accumulate the secondary sort values
	void Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk, idx_t input_idx,
	          OperatorSinkInput &sink) override;
	//! Finish the sinking and prepare to scan
	void Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) override;

	void NextRank(idx_t partition_begin, idx_t peer_begin, idx_t row_idx);

	idx_t row_idx = DConstants::INVALID_INDEX;
	uint64_t dense_rank = 1;
	uint64_t rank_equal = 0;
	uint64_t rank = 1;

	//! The corresponding global peer state
	const WindowPeerGlobalState &gpstate;
	//! The optional sorting state for secondary sorts
	unique_ptr<LocalSinkState> local_tree;
};

void WindowPeerLocalState::Sink(ExecutionContext &context, DataChunk &sink_chunk, DataChunk &coll_chunk,
                                idx_t input_idx, OperatorSinkInput &sink) {
	WindowExecutorBoundsLocalState::Sink(context, sink_chunk, coll_chunk, input_idx, sink);

	if (local_tree) {
		auto &local_tokens = local_tree->Cast<WindowMergeSortTreeLocalState>();
		local_tokens.Sink(context, sink_chunk, input_idx, nullptr, 0, sink.interrupt_state);
	}
}

void WindowPeerLocalState::Finalize(ExecutionContext &context, CollectionPtr collection, OperatorSinkInput &sink) {
	WindowExecutorBoundsLocalState::Finalize(context, collection, sink);

	if (local_tree) {
		auto &local_tokens = local_tree->Cast<WindowMergeSortTreeLocalState>();
		local_tokens.Finalize(context, sink.interrupt_state);
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
WindowPeerExecutor::WindowPeerExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : WindowExecutor(wexpr, shared) {
	for (const auto &order : wexpr.arg_orders) {
		arg_order_idx.emplace_back(shared.RegisterSink(order.expression));
	}
}

unique_ptr<GlobalSinkState> WindowPeerExecutor::GetGlobalState(ClientContext &client, const idx_t payload_count,
                                                               const ValidityMask &partition_mask,
                                                               const ValidityMask &order_mask) const {
	return make_uniq<WindowPeerGlobalState>(client, *this, payload_count, partition_mask, order_mask);
}

unique_ptr<LocalSinkState> WindowPeerExecutor::GetLocalState(ExecutionContext &context,
                                                             const GlobalSinkState &gstate) const {
	return make_uniq<WindowPeerLocalState>(context, gstate.Cast<WindowPeerGlobalState>());
}

//===--------------------------------------------------------------------===//
// WindowRankExecutor
//===--------------------------------------------------------------------===//
WindowRankExecutor::WindowRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : WindowPeerExecutor(wexpr, shared) {
}

void WindowRankExecutor::EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result, idx_t count,
                                          idx_t row_idx, OperatorSinkInput &sink) const {
	auto &gpeer = sink.global_state.Cast<WindowPeerGlobalState>();
	auto &lpeer = sink.local_state.Cast<WindowPeerLocalState>();
	auto rdata = FlatVector::GetData<int64_t>(result);

	if (gpeer.use_framing) {
		auto frame_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_BEGIN]);
		auto frame_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_END]);
		if (gpeer.token_tree) {
			for (idx_t i = 0; i < count; ++i, ++row_idx) {
				rdata[i] = UnsafeNumericCast<int64_t>(gpeer.token_tree->Rank(frame_begin[i], frame_end[i], row_idx));
			}
		} else {
			auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
			for (idx_t i = 0; i < count; ++i, ++row_idx) {
				//	Clamp peer to the frame
				const auto frame_peer_begin = MaxValue(frame_begin[i], peer_begin[i]);
				rdata[i] = UnsafeNumericCast<int64_t>((frame_peer_begin - frame_begin[i]) + 1);
			}
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
		rdata[i] = UnsafeNumericCast<int64_t>(lpeer.rank);
	}
}

//===--------------------------------------------------------------------===//
// WindowDenseRankExecutor
//===--------------------------------------------------------------------===//
WindowDenseRankExecutor::WindowDenseRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : WindowPeerExecutor(wexpr, shared) {
}

void WindowDenseRankExecutor::EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result,
                                               idx_t count, idx_t row_idx, OperatorSinkInput &sink) const {
	auto &gpeer = sink.global_state.Cast<WindowPeerGlobalState>();
	auto &lpeer = sink.local_state.Cast<WindowPeerLocalState>();

	auto &order_mask = gpeer.order_mask;
	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
	auto rdata = FlatVector::GetData<int64_t>(result);

	//	Reset to "previous" row
	//	Resetting is slow because we have to rescan the mask.
	//	So check whether we are just picking up where we left off.
	//	This is common because the main window operator
	//	evaluates maximally sized runs for each hash group.
	if (lpeer.row_idx != row_idx) {
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
	}

	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		lpeer.NextRank(partition_begin[i], peer_begin[i], row_idx);
		rdata[i] = NumericCast<int64_t>(lpeer.dense_rank);
	}

	//	Remember where we left off
	lpeer.row_idx = row_idx;
}

//===--------------------------------------------------------------------===//
// WindowPercentRankExecutor
//===--------------------------------------------------------------------===//
WindowPercentRankExecutor::WindowPercentRankExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : WindowPeerExecutor(wexpr, shared) {
}

static inline double PercentRank(const idx_t begin, const idx_t end, const uint64_t rank) {
	auto denom = static_cast<double>(NumericCast<int64_t>(end - begin - 1));
	return denom > 0 ? ((double)rank - 1) / denom : 0;
}

void WindowPercentRankExecutor::EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result,
                                                 idx_t count, idx_t row_idx, OperatorSinkInput &sink) const {
	auto &gpeer = sink.global_state.Cast<WindowPeerGlobalState>();
	auto &lpeer = sink.local_state.Cast<WindowPeerLocalState>();
	auto rdata = FlatVector::GetData<double>(result);

	if (gpeer.use_framing) {
		auto frame_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_BEGIN]);
		auto frame_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_END]);
		if (gpeer.token_tree) {
			for (idx_t i = 0; i < count; ++i, ++row_idx) {
				const auto rank = gpeer.token_tree->Rank(frame_begin[i], frame_end[i], row_idx);
				rdata[i] = PercentRank(frame_begin[i], frame_end[i], rank);
			}
		} else {
			//	Clamp peer to the frame
			auto peer_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_BEGIN]);
			for (idx_t i = 0; i < count; ++i, ++row_idx) {
				const auto frame_peer_begin = MaxValue(frame_begin[i], peer_begin[i]);
				lpeer.rank = (frame_peer_begin - frame_begin[i]) + 1;
				rdata[i] = PercentRank(frame_begin[i], frame_end[i], lpeer.rank);
			}
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
		rdata[i] = PercentRank(partition_begin[i], partition_end[i], lpeer.rank);
	}
}

//===--------------------------------------------------------------------===//
// WindowCumeDistExecutor
//===--------------------------------------------------------------------===//
WindowCumeDistExecutor::WindowCumeDistExecutor(BoundWindowExpression &wexpr, WindowSharedExpressions &shared)
    : WindowPeerExecutor(wexpr, shared) {
}

static inline double CumeDist(const idx_t begin, const idx_t end, const idx_t peer_end) {
	const auto denom = static_cast<double>(NumericCast<int64_t>(end - begin));
	const auto num = static_cast<double>(peer_end - begin);
	return denom > 0 ? (num / denom) : 0;
}

void WindowCumeDistExecutor::EvaluateInternal(ExecutionContext &context, DataChunk &eval_chunk, Vector &result,
                                              idx_t count, idx_t row_idx, OperatorSinkInput &sink) const {
	auto &gpeer = sink.global_state.Cast<WindowPeerGlobalState>();
	auto &lpeer = sink.local_state.Cast<WindowPeerLocalState>();
	auto rdata = FlatVector::GetData<double>(result);

	if (gpeer.use_framing) {
		auto frame_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_BEGIN]);
		auto frame_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[FRAME_END]);
		if (gpeer.token_tree) {
			for (idx_t i = 0; i < count; ++i, ++row_idx) {
				const auto peer_end = gpeer.token_tree->PeerEnd(frame_begin[i], frame_end[i], row_idx);
				rdata[i] = CumeDist(frame_begin[i], frame_end[i], peer_end);
			}
		} else {
			auto peer_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_END]);
			for (idx_t i = 0; i < count; ++i, ++row_idx) {
				//	Clamp the peer end to the frame
				const auto frame_peer_end = MinValue(peer_end[i], frame_end[i]);
				rdata[i] = CumeDist(frame_begin[i], frame_end[i], frame_peer_end);
			}
		}
		return;
	}

	auto partition_begin = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_BEGIN]);
	auto partition_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PARTITION_END]);
	auto peer_end = FlatVector::GetData<const idx_t>(lpeer.bounds.data[PEER_END]);
	for (idx_t i = 0; i < count; ++i, ++row_idx) {
		rdata[i] = CumeDist(partition_begin[i], partition_end[i], peer_end[i]);
	}
}

} // namespace duckdb
