#include "duckdb/execution/operator/join/physical_right_delim_join.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalRightDelimJoin::PhysicalRightDelimJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
                                               vector<const_reference<PhysicalOperator>> delim_scans,
                                               idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::LEFT_DELIM_JOIN, std::move(types), estimated_cardinality),
      join(std::move(original_join)), delim_scans(std::move(delim_scans)) {
	D_ASSERT(join->children.size() == 2);
	// now for the original join
	// we take its right child, this is the side that we will duplicate eliminate
	children.push_back(std::move(join->children[1]));

	// we replace it with a PhysicalColumnDataScan, that scans the ColumnDataCollection that we keep cached
	// the actual chunk collection to scan will be created in the RightDelimJoinGlobalState
	auto cached_chunk_scan = make_uniq<PhysicalColumnDataScan>(
	    children[0]->GetTypes(), PhysicalOperatorType::COLUMN_DATA_SCAN, estimated_cardinality);
	join->children[1] = std::move(cached_chunk_scan);
}

vector<const_reference<PhysicalOperator>> PhysicalRightDelimJoin::GetChildren() const {
	vector<const_reference<PhysicalOperator>> result;
	for (auto &child : children) {
		result.push_back(*child);
	}
	result.push_back(*join);
	result.push_back(*distinct);
	return result;
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

string PhysicalRightDelimJoin::ParamsToString() const {
	return join->ParamsToString();
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalRightDelimJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(*children[0]);

	if (type == PhysicalOperatorType::RIGHT_DELIM_JOIN) {
		// recurse into the actual join
		// any pipelines in there depend on the main pipeline
		// any scan of the duplicate eliminated data on the RHS depends on this pipeline
		// we add an entry to the mapping of (PhysicalOperator*) -> (Pipeline*)
		auto &state = meta_pipeline.GetState();
		for (auto &delim_scan : delim_scans) {
			state.delim_join_dependencies.insert(
			    make_pair(delim_scan, reference<Pipeline>(*child_meta_pipeline.GetBasePipeline())));
		}
		join->BuildPipelines(current, meta_pipeline);
	}
}

} // namespace duckdb
