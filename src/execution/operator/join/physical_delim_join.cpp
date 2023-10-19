#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_column_data_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"

namespace duckdb {

PhysicalDelimJoin::PhysicalDelimJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
                                     vector<const_reference<PhysicalOperator>> delim_scans, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::DELIM_JOIN, std::move(types), estimated_cardinality),
      join(std::move(original_join)), delim_scans(std::move(delim_scans)) {
	D_ASSERT(join->children.size() == 2);
	// now for the original join
	// we take its left child, this is the side that we will duplicate eliminate
	children.push_back(std::move(join->children[0]));

	// we replace it with a PhysicalColumnDataScan, that scans the ColumnDataCollection that we keep cached
	// the actual chunk collection to scan will be created in the DelimJoinGlobalState
	auto cached_chunk_scan = make_uniq<PhysicalColumnDataScan>(
	    children[0]->GetTypes(), PhysicalOperatorType::COLUMN_DATA_SCAN, estimated_cardinality);
	join->children[0] = std::move(cached_chunk_scan);
}

vector<const_reference<PhysicalOperator>> PhysicalDelimJoin::GetChildren() const {
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
class DelimJoinGlobalState : public GlobalSinkState {
public:
	explicit DelimJoinGlobalState(ClientContext &context, const PhysicalDelimJoin &delim_join)
	    : lhs_data(context, delim_join.children[0]->GetTypes()) {
		D_ASSERT(delim_join.delim_scans.size() > 0);
		// set up the delim join chunk to scan in the original join
		auto &cached_chunk_scan = delim_join.join->children[0]->Cast<PhysicalColumnDataScan>();
		cached_chunk_scan.collection = &lhs_data;
	}

	ColumnDataCollection lhs_data;
	mutex lhs_lock;

	void Merge(ColumnDataCollection &input) {
		lock_guard<mutex> guard(lhs_lock);
		lhs_data.Combine(input);
	}
};

class DelimJoinLocalState : public LocalSinkState {
public:
	explicit DelimJoinLocalState(ClientContext &context, const PhysicalDelimJoin &delim_join)
	    : lhs_data(context, delim_join.children[0]->GetTypes()) {
		lhs_data.InitializeAppend(append_state);
	}

	unique_ptr<LocalSinkState> distinct_state;
	ColumnDataCollection lhs_data;
	ColumnDataAppendState append_state;

	void Append(DataChunk &input) {
		lhs_data.Append(input);
	}
};

unique_ptr<GlobalSinkState> PhysicalDelimJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<DelimJoinGlobalState>(context, *this);
	distinct->sink_state = distinct->GetGlobalSinkState(context);
	if (delim_scans.size() > 1) {
		PhysicalHashAggregate::SetMultiScan(*distinct->sink_state);
	}
	return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalDelimJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<DelimJoinLocalState>(context.client, *this);
	state->distinct_state = distinct->GetLocalSinkState(context);
	return std::move(state);
}

SinkResultType PhysicalDelimJoin::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &lstate = input.local_state.Cast<DelimJoinLocalState>();
	lstate.lhs_data.Append(lstate.append_state, chunk);
	OperatorSinkInput distinct_sink_input {*distinct->sink_state, *lstate.distinct_state, input.interrupt_state};
	distinct->Sink(context, chunk, distinct_sink_input);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalDelimJoin::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<DelimJoinLocalState>();
	auto &gstate = input.global_state.Cast<DelimJoinGlobalState>();
	gstate.Merge(lstate.lhs_data);

	OperatorSinkCombineInput distinct_combine_input {*distinct->sink_state, *lstate.distinct_state,
	                                                 input.interrupt_state};
	distinct->Combine(context, distinct_combine_input);

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalDelimJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &client,
                                             OperatorSinkFinalizeInput &input) const {
	// finalize the distinct HT
	D_ASSERT(distinct);

	OperatorSinkFinalizeInput finalize_input {*distinct->sink_state, input.interrupt_state};
	distinct->Finalize(pipeline, event, client, finalize_input);
	return SinkFinalizeType::READY;
}

string PhysicalDelimJoin::ParamsToString() const {
	return join->ParamsToString();
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalDelimJoin::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(*children[0]);

	if (type == PhysicalOperatorType::DELIM_JOIN) {
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
