#include "duckdb/execution/operator/join/physical_right_delim_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/join/physical_join.hpp"
#include "duckdb/execution/operator/scan/physical_dummy_scan.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalRightDelimJoin::PhysicalRightDelimJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
                                               vector<const_reference<PhysicalOperator>> delim_scans,
                                               idx_t estimated_cardinality, optional_idx delim_idx)
    : PhysicalDelimJoin(PhysicalOperatorType::RIGHT_DELIM_JOIN, std::move(types), std::move(original_join),
                        std::move(delim_scans), estimated_cardinality, delim_idx) {
	D_ASSERT(join->children.size() == 2);
	// now for the original join
	// we take its right child, this is the side that we will duplicate eliminate
	children.push_back(std::move(join->children[1]));

	// we replace it with a PhysicalDummyScan, which contains no data, just the types, it won't be scanned anyway
	join->children[1] = make_uniq<PhysicalDummyScan>(children[0]->GetTypes(), estimated_cardinality);
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class RightDelimJoinGlobalState : public GlobalSinkState {};

class RightDelimJoinLocalState : public LocalSinkState {
public:
	unique_ptr<LocalSinkState> join_state;
	unique_ptr<LocalSinkState> distinct_state;
};

unique_ptr<GlobalSinkState> PhysicalRightDelimJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<RightDelimJoinGlobalState>();
	join->sink_state = join->GetGlobalSinkState(context);
	distinct->sink_state = distinct->GetGlobalSinkState(context);
	if (delim_scans.size() > 1) {
		PhysicalHashAggregate::SetMultiScan(*distinct->sink_state);
	}
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalRightDelimJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<RightDelimJoinLocalState>();
	state->join_state = join->GetLocalSinkState(context);
	state->distinct_state = distinct->GetLocalSinkState(context);
	return std::move(state);
}

SinkResultType PhysicalRightDelimJoin::Sink(ExecutionContext &context, DataChunk &chunk,
                                            OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<RightDelimJoinLocalState>();

	OperatorSinkInput join_sink_input {*join->sink_state, *lstate.join_state, input.interrupt_state};
	join->Sink(context, chunk, join_sink_input);

	OperatorSinkInput distinct_sink_input {*distinct->sink_state, *lstate.distinct_state, input.interrupt_state};
	distinct->Sink(context, chunk, distinct_sink_input);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalRightDelimJoin::Combine(ExecutionContext &context,
                                                      OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<RightDelimJoinLocalState>();

	OperatorSinkCombineInput join_combine_input {*join->sink_state, *lstate.join_state, input.interrupt_state};
	join->Combine(context, join_combine_input);

	OperatorSinkCombineInput distinct_combine_input {*distinct->sink_state, *lstate.distinct_state,
	                                                 input.interrupt_state};
	distinct->Combine(context, distinct_combine_input);

	return SinkCombineResultType::FINISHED;
}

void PhysicalRightDelimJoin::PrepareFinalize(ClientContext &context, GlobalSinkState &sink_state) const {
	join->PrepareFinalize(context, *join->sink_state);
	distinct->PrepareFinalize(context, *distinct->sink_state);
}

SinkFinalizeType PhysicalRightDelimJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &client,
                                                  OperatorSinkFinalizeInput &input) const {
	D_ASSERT(join);
	D_ASSERT(distinct);

	OperatorSinkFinalizeInput join_finalize_input {*join->sink_state, input.interrupt_state};
	join->Finalize(pipeline, event, client, join_finalize_input);

	OperatorSinkFinalizeInput distinct_finalize_input {*distinct->sink_state, input.interrupt_state};
	distinct->Finalize(pipeline, event, client, distinct_finalize_input);

	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalRightDelimJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(*children[0]);

	D_ASSERT(type == PhysicalOperatorType::RIGHT_DELIM_JOIN);
	// recurse into the actual join
	// any pipelines in there depend on the main pipeline
	// any scan of the duplicate eliminated data on the LHS depends on this pipeline
	// we add an entry to the mapping of (PhysicalOperator*) -> (Pipeline*)
	auto &state = meta_pipeline.GetState();
	for (auto &delim_scan : delim_scans) {
		state.delim_join_dependencies.insert(
		    make_pair(delim_scan, reference<Pipeline>(*child_meta_pipeline.GetBasePipeline())));
	}

	// Build join pipelines without building the RHS (already built in the Sink of this op)
	PhysicalJoin::BuildJoinPipelines(current, meta_pipeline, *join, false);
}

} // namespace duckdb
