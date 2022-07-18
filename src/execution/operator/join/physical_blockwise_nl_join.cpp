#include "duckdb/execution/operator/join/physical_blockwise_nl_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/execution/operator/join/physical_comparison_join.hpp"
#include "duckdb/common/types/column_data_collection.hpp"
#include "duckdb/execution/operator/join/outer_join_marker.hpp"
#include "duckdb/execution/operator/join/physical_cross_product.hpp"

namespace duckdb {

PhysicalBlockwiseNLJoin::PhysicalBlockwiseNLJoin(LogicalOperator &op, unique_ptr<PhysicalOperator> left,
                                                 unique_ptr<PhysicalOperator> right, unique_ptr<Expression> condition,
                                                 JoinType join_type, idx_t estimated_cardinality)
    : PhysicalJoin(op, PhysicalOperatorType::BLOCKWISE_NL_JOIN, join_type, estimated_cardinality),
      condition(move(condition)) {
	children.push_back(move(left));
	children.push_back(move(right));
	// MARK and SINGLE joins not handled
	D_ASSERT(join_type != JoinType::MARK);
	D_ASSERT(join_type != JoinType::SINGLE);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinLocalState : public LocalSinkState {
public:
	BlockwiseNLJoinLocalState() {
	}
};

class BlockwiseNLJoinGlobalState : public GlobalSinkState {
public:
	explicit BlockwiseNLJoinGlobalState(ClientContext &context, const PhysicalBlockwiseNLJoin &op)
	    : right_chunks(context, op.children[1]->GetTypes()), right_outer(IsRightOuterJoin(op.join_type)) {
	}

	mutex lock;
	ColumnDataCollection right_chunks;
	OuterJoinMarker right_outer;
};

unique_ptr<GlobalSinkState> PhysicalBlockwiseNLJoin::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<BlockwiseNLJoinGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalBlockwiseNLJoin::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<BlockwiseNLJoinLocalState>();
}

SinkResultType PhysicalBlockwiseNLJoin::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                             DataChunk &input) const {
	auto &gstate = (BlockwiseNLJoinGlobalState &)state;
	lock_guard<mutex> nl_lock(gstate.lock);
	gstate.right_chunks.Append(input);
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
SinkFinalizeType PhysicalBlockwiseNLJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                   GlobalSinkState &gstate_p) const {
	auto &gstate = (BlockwiseNLJoinGlobalState &)gstate_p;
	gstate.right_outer.Initialize(gstate.right_chunks.Count());

	if (gstate.right_chunks.Count() == 0 && EmptyResultIfRHSIsEmpty()) {
		return SinkFinalizeType::NO_OUTPUT_POSSIBLE;
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinState : public OperatorState {
public:
	explicit BlockwiseNLJoinState(ExecutionContext &context, ColumnDataCollection &rhs,
	                              const PhysicalBlockwiseNLJoin &op)
	    : cross_product(rhs), left_outer(IsLeftOuterJoin(op.join_type)), match_sel(STANDARD_VECTOR_SIZE),
	      executor(Allocator::Get(context.client), *op.condition) {
		left_outer.Initialize(STANDARD_VECTOR_SIZE);
	}

	CrossProductExecutor cross_product;
	OuterJoinMarker left_outer;
	SelectionVector match_sel;
	ExpressionExecutor executor;
};

unique_ptr<OperatorState> PhysicalBlockwiseNLJoin::GetOperatorState(ExecutionContext &context) const {
	auto &gstate = (BlockwiseNLJoinGlobalState &)*sink_state;
	return make_unique<BlockwiseNLJoinState>(context, gstate.right_chunks, *this);
}

OperatorResultType PhysicalBlockwiseNLJoin::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate_p, OperatorState &state_p) const {
	D_ASSERT(input.size() > 0);
	auto &state = (BlockwiseNLJoinState &)state_p;
	auto &gstate = (BlockwiseNLJoinGlobalState &)*sink_state;

	if (gstate.right_chunks.Count() == 0) {
		// empty RHS
		if (!EmptyResultIfRHSIsEmpty()) {
			PhysicalComparisonJoin::ConstructEmptyJoinResult(join_type, false, input, chunk);
			return OperatorResultType::NEED_MORE_INPUT;
		} else {
			return OperatorResultType::FINISHED;
		}
	}

	// now perform the actual join
	// we perform a cross product, then execute the expression directly on the cross product' result
	idx_t result_count = 0;
	do {
		auto result = state.cross_product.Execute(input, chunk);
		if (result == OperatorResultType::NEED_MORE_INPUT) {
			// exhausted input, have to pull new LHS chunk
			if (state.left_outer.Enabled()) {
				// left join: before we move to the next chunk, see if we need to output any vectors that didn't
				// have a match found
				state.left_outer.ConstructLeftJoinResult(input, chunk);
				state.left_outer.Reset();
			}
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// now perform the computation
		result_count = state.executor.SelectExpression(chunk, state.match_sel);
		if (result_count > 0) {
			// found a match!
			// check if the cross product is scanning the LHS or the RHS in its entirety
			if (!state.cross_product.ScanLHS()) {
				// set the match flags in the LHS
				state.left_outer.SetMatches(state.match_sel, result_count);
				// set the match flag in the RHS
				gstate.right_outer.SetMatch(state.cross_product.ScanPosition() + state.cross_product.PositionInChunk());
			} else {
				// set the match flag in the LHS
				state.left_outer.SetMatch(state.cross_product.PositionInChunk());
				// set the match flags in the RHS
				gstate.right_outer.SetMatches(state.match_sel, result_count, state.cross_product.ScanPosition());
			}
			chunk.Slice(state.match_sel, result_count);
		} else {
			// no result: reset the chunk
			chunk.Reset();
		}
	} while (result_count == 0);
	return OperatorResultType::HAVE_MORE_OUTPUT;
}

string PhysicalBlockwiseNLJoin::ParamsToString() const {
	string extra_info = JoinTypeToString(join_type) + "\n";
	extra_info += condition->GetName();
	return extra_info;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
class BlockwiseNLJoinGlobalScanState : public GlobalSourceState {
public:
	explicit BlockwiseNLJoinGlobalScanState(const PhysicalBlockwiseNLJoin &op) : op(op) {
		D_ASSERT(op.sink_state);
		auto &sink = (BlockwiseNLJoinGlobalState &)*op.sink_state;
		sink.right_outer.InitializeScan(sink.right_chunks, scan_state);
	}

	const PhysicalBlockwiseNLJoin &op;
	OuterJoinGlobalScanState scan_state;

public:
	idx_t MaxThreads() override {
		auto &sink = (BlockwiseNLJoinGlobalState &)*op.sink_state;
		return sink.right_outer.MaxThreads();
	}
};

class BlockwiseNLJoinLocalScanState : public LocalSourceState {
public:
	explicit BlockwiseNLJoinLocalScanState(const PhysicalBlockwiseNLJoin &op, BlockwiseNLJoinGlobalScanState &gstate) {
		D_ASSERT(op.sink_state);
		auto &sink = (BlockwiseNLJoinGlobalState &)*op.sink_state;
		sink.right_outer.InitializeScan(gstate.scan_state, scan_state);
	}

	OuterJoinLocalScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalBlockwiseNLJoin::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<BlockwiseNLJoinGlobalScanState>(*this);
}

unique_ptr<LocalSourceState> PhysicalBlockwiseNLJoin::GetLocalSourceState(ExecutionContext &context,
                                                                          GlobalSourceState &gstate) const {
	return make_unique<BlockwiseNLJoinLocalScanState>(*this, (BlockwiseNLJoinGlobalScanState &)gstate);
}

void PhysicalBlockwiseNLJoin::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                      LocalSourceState &lstate_p) const {
	D_ASSERT(IsRightOuterJoin(join_type));
	// check if we need to scan any unmatched tuples from the RHS for the full/right outer join
	auto &sink = (BlockwiseNLJoinGlobalState &)*sink_state;
	auto &gstate = (BlockwiseNLJoinGlobalScanState &)gstate_p;
	auto &lstate = (BlockwiseNLJoinLocalScanState &)lstate_p;

	// if the LHS is exhausted in a FULL/RIGHT OUTER JOIN, we scan chunks we still need to output
	sink.right_outer.Scan(gstate.scan_state, lstate.scan_state, chunk);
}

} // namespace duckdb
