#include "duckdb/execution/operator/join/physical_delim_join.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/operator/scan/physical_chunk_scan.hpp"
#include "duckdb/execution/operator/aggregate/physical_hash_aggregate.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

PhysicalDelimJoin::PhysicalDelimJoin(vector<LogicalType> types, unique_ptr<PhysicalOperator> original_join,
                                     vector<PhysicalOperator *> delim_scans, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::DELIM_JOIN, move(types), estimated_cardinality), join(move(original_join)),
      delim_scans(move(delim_scans)) {
	D_ASSERT(join->children.size() == 2);
	// now for the original join
	// we take its left child, this is the side that we will duplicate eliminate
	children.push_back(move(join->children[0]));

	// we replace it with a PhysicalChunkCollectionScan, that scans the ChunkCollection that we keep cached
	// the actual chunk collection to scan will be created in the DelimJoinGlobalState
	auto cached_chunk_scan = make_unique<PhysicalChunkScan>(children[0]->GetTypes(), PhysicalOperatorType::CHUNK_SCAN,
	                                                        estimated_cardinality);
	join->children[0] = move(cached_chunk_scan);
}

vector<PhysicalOperator *> PhysicalDelimJoin::GetChildren() const {
	vector<PhysicalOperator *> result;
	for (auto &child : children) {
		result.push_back(child.get());
	}
	result.push_back(join.get());
	result.push_back(distinct.get());
	return result;
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class DelimJoinGlobalState : public GlobalSinkState {
public:
	explicit DelimJoinGlobalState(Allocator &allocator, const PhysicalDelimJoin *delim_join) : lhs_data(allocator) {
		D_ASSERT(delim_join->delim_scans.size() > 0);
		// set up the delim join chunk to scan in the original join
		auto &cached_chunk_scan = (PhysicalChunkScan &)*delim_join->join->children[0];
		cached_chunk_scan.collection = &lhs_data;
	}

	ChunkCollection lhs_data;
	mutex lhs_lock;

	void Merge(ChunkCollection &input) {
		lock_guard<mutex> guard(lhs_lock);
		lhs_data.Append(input);
	}
};

class DelimJoinLocalState : public LocalSinkState {
public:
	explicit DelimJoinLocalState(Allocator &allocator) : lhs_data(allocator) {
	}

	unique_ptr<LocalSinkState> distinct_state;
	ChunkCollection lhs_data;

	void Append(DataChunk &input) {
		lhs_data.Append(input);
	}
};

unique_ptr<GlobalSinkState> PhysicalDelimJoin::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_unique<DelimJoinGlobalState>(BufferAllocator::Get(context), this);
	distinct->sink_state = distinct->GetGlobalSinkState(context);
	if (delim_scans.size() > 1) {
		PhysicalHashAggregate::SetMultiScan(*distinct->sink_state);
	}
	return move(state);
}

unique_ptr<LocalSinkState> PhysicalDelimJoin::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_unique<DelimJoinLocalState>(Allocator::Get(context.client));
	state->distinct_state = distinct->GetLocalSinkState(context);
	return move(state);
}

SinkResultType PhysicalDelimJoin::Sink(ExecutionContext &context, GlobalSinkState &state_p, LocalSinkState &lstate_p,
                                       DataChunk &input) const {
	auto &lstate = (DelimJoinLocalState &)lstate_p;
	lstate.lhs_data.Append(input);
	distinct->Sink(context, *distinct->sink_state, *lstate.distinct_state, input);
	return SinkResultType::NEED_MORE_INPUT;
}

void PhysicalDelimJoin::Combine(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate_p) const {
	auto &lstate = (DelimJoinLocalState &)lstate_p;
	auto &gstate = (DelimJoinGlobalState &)state;
	gstate.Merge(lstate.lhs_data);
	distinct->Combine(context, *distinct->sink_state, *lstate.distinct_state);
}

SinkFinalizeType PhysicalDelimJoin::Finalize(Pipeline &pipeline, Event &event, ClientContext &client,
                                             GlobalSinkState &gstate) const {
	// finalize the distinct HT
	D_ASSERT(distinct);
	distinct->Finalize(pipeline, event, client, *distinct->sink_state);
	return SinkFinalizeType::READY;
}

string PhysicalDelimJoin::ParamsToString() const {
	return join->ParamsToString();
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalDelimJoin::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	op_state.reset();
	sink_state.reset();

	// duplicate eliminated join
	auto pipeline = make_shared<Pipeline>(executor);
	state.SetPipelineSink(*pipeline, this);
	current.AddDependency(pipeline);

	// recurse into the pipeline child
	children[0]->BuildPipelines(executor, *pipeline, state);
	if (type == PhysicalOperatorType::DELIM_JOIN) {
		// recurse into the actual join
		// any pipelines in there depend on the main pipeline
		// any scan of the duplicate eliminated data on the RHS depends on this pipeline
		// we add an entry to the mapping of (PhysicalOperator*) -> (Pipeline*)
		for (auto &delim_scan : delim_scans) {
			state.delim_join_dependencies[delim_scan] = pipeline.get();
		}
		join->BuildPipelines(executor, current, state);
	}
	if (!state.recursive_cte) {
		// regular pipeline: schedule it
		state.AddPipeline(executor, move(pipeline));
	} else {
		// CTE pipeline! add it to the CTE pipelines
		auto &cte = (PhysicalRecursiveCTE &)*state.recursive_cte;
		cte.pipelines.push_back(move(pipeline));
	}
}

} // namespace duckdb
