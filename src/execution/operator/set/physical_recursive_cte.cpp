#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

#include "duckdb/common/vector_operations/vector_operations.hpp"

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/parallel/event.hpp"

namespace duckdb {

PhysicalRecursiveCTE::PhysicalRecursiveCTE(vector<LogicalType> types, bool union_all, unique_ptr<PhysicalOperator> top,
                                           unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::RECURSIVE_CTE, move(types), estimated_cardinality), union_all(union_all) {
	children.push_back(move(top));
	children.push_back(move(bottom));
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class RecursiveCTEState : public GlobalSinkState {
public:
	explicit RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : intermediate_table(Allocator::Get(context)), new_groups(STANDARD_VECTOR_SIZE) {
		ht = make_unique<GroupedAggregateHashTable>(Allocator::Get(context), BufferManager::GetBufferManager(context),
		                                            op.types, vector<LogicalType>(),
		                                            vector<BoundAggregateExpression *>());
	}

	unique_ptr<GroupedAggregateHashTable> ht;

	bool intermediate_empty = true;
	ChunkCollection intermediate_table;
	idx_t chunk_idx = 0;
	SelectionVector new_groups;
};

unique_ptr<GlobalSinkState> PhysicalRecursiveCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<RecursiveCTEState>(context, *this);
}

idx_t PhysicalRecursiveCTE::ProbeHT(DataChunk &chunk, RecursiveCTEState &state) const {
	Vector dummy_addresses(LogicalType::POINTER);

	// Use the HT to eliminate duplicate rows
	idx_t new_group_count = state.ht->FindOrCreateGroups(chunk, dummy_addresses, state.new_groups);

	// we only return entries we have not seen before (i.e. new groups)
	chunk.Slice(state.new_groups, new_group_count);

	return new_group_count;
}

SinkResultType PhysicalRecursiveCTE::Sink(ExecutionContext &context, GlobalSinkState &state, LocalSinkState &lstate,
                                          DataChunk &input) const {
	auto &gstate = (RecursiveCTEState &)state;
	if (!union_all) {
		idx_t match_count = ProbeHT(input, gstate);
		if (match_count > 0) {
			gstate.intermediate_table.Append(input);
		}
	} else {
		gstate.intermediate_table.Append(input);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
void PhysicalRecursiveCTE::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate_p,
                                   LocalSourceState &lstate) const {
	auto &gstate = (RecursiveCTEState &)*sink_state;
	while (chunk.size() == 0) {
		if (gstate.chunk_idx < gstate.intermediate_table.ChunkCount()) {
			// scan any chunks we have collected so far
			chunk.Reference(gstate.intermediate_table.GetChunk(gstate.chunk_idx));
			gstate.chunk_idx++;
			break;
		} else {
			// we have run out of chunks
			// now we need to recurse
			// we set up the working table as the data we gathered in this iteration of the recursion
			working_table->Reset();
			working_table->Merge(gstate.intermediate_table);
			// and we clear the intermediate table
			gstate.intermediate_table.Reset();
			gstate.chunk_idx = 0;
			// now we need to re-execute all of the pipelines that depend on the recursion
			ExecuteRecursivePipelines(context);

			// check if we obtained any results
			// if not, we are done
			if (gstate.intermediate_table.Count() == 0) {
				break;
			}
		}
	}
}

void PhysicalRecursiveCTE::ExecuteRecursivePipelines(ExecutionContext &context) const {
	if (pipelines.empty()) {
		throw InternalException("Missing pipelines for recursive CTE");
	}

	for (auto &pipeline : pipelines) {
		auto sink = pipeline->GetSink();
		if (sink != this) {
			// reset the sink state for any intermediate sinks
			sink->sink_state = sink->GetGlobalSinkState(context.client);
		}
		for (auto &op : pipeline->GetOperators()) {
			if (op) {
				op->op_state = op->GetGlobalOperatorState(context.client);
			}
		}
		pipeline->Reset();
	}
	auto &executor = pipelines[0]->executor;

	vector<shared_ptr<Event>> events;
	executor.ReschedulePipelines(pipelines, events);

	while (true) {
		executor.WorkOnTasks();
		if (executor.HasError()) {
			executor.ThrowException();
		}
		bool finished = true;
		for (auto &event : events) {
			if (!event->IsFinished()) {
				finished = false;
				break;
			}
		}
		if (finished) {
			// all pipelines finished: done!
			break;
		}
	}
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalRecursiveCTE::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	op_state.reset();
	sink_state.reset();

	// recursive CTE
	state.SetPipelineSource(current, this);
	// the LHS of the recursive CTE is our initial state
	// we build this pipeline as normal
	auto pipeline_child = children[0].get();
	// for the RHS, we gather all pipelines that depend on the recursive cte
	// these pipelines need to be rerun
	if (state.recursive_cte) {
		throw InternalException("Recursive CTE detected WITHIN a recursive CTE node");
	}
	state.recursive_cte = this;

	auto recursive_pipeline = make_shared<Pipeline>(executor);
	state.SetPipelineSink(*recursive_pipeline, this);
	children[1]->BuildPipelines(executor, *recursive_pipeline, state);

	pipelines.push_back(move(recursive_pipeline));

	state.recursive_cte = nullptr;

	BuildChildPipeline(executor, current, state, pipeline_child);
}

vector<const PhysicalOperator *> PhysicalRecursiveCTE::GetSources() const {
	return {this};
}

} // namespace duckdb
