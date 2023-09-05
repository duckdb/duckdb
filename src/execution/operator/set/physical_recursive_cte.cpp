#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/execution/aggregate_hashtable.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/parallel/event.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PhysicalRecursiveCTE::PhysicalRecursiveCTE(string ctename, idx_t table_index, vector<LogicalType> types, bool union_all,
                                           unique_ptr<PhysicalOperator> top, unique_ptr<PhysicalOperator> bottom,
                                           idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::RECURSIVE_CTE, std::move(types), estimated_cardinality),
      ctename(std::move(ctename)), table_index(table_index), union_all(union_all) {
	children.push_back(std::move(top));
	children.push_back(std::move(bottom));
}

PhysicalRecursiveCTE::~PhysicalRecursiveCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class RecursiveCTEState : public GlobalSinkState {
public:
	explicit RecursiveCTEState(ClientContext &context, const PhysicalRecursiveCTE &op)
	    : intermediate_table(context, op.GetTypes()), new_groups(STANDARD_VECTOR_SIZE) {
		ht = make_uniq<GroupedAggregateHashTable>(context, BufferAllocator::Get(context), op.types,
		                                          vector<LogicalType>(), vector<BoundAggregateExpression *>());
	}

	unique_ptr<GroupedAggregateHashTable> ht;

	bool intermediate_empty = true;
	ColumnDataCollection intermediate_table;
	ColumnDataScanState scan_state;
	bool initialized = false;
	bool finished_scan = false;
	SelectionVector new_groups;
};

unique_ptr<GlobalSinkState> PhysicalRecursiveCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<RecursiveCTEState>(context, *this);
}

idx_t PhysicalRecursiveCTE::ProbeHT(DataChunk &chunk, RecursiveCTEState &state) const {
	Vector dummy_addresses(LogicalType::POINTER);

	// Use the HT to eliminate duplicate rows
	idx_t new_group_count = state.ht->FindOrCreateGroups(chunk, dummy_addresses, state.new_groups);

	// we only return entries we have not seen before (i.e. new groups)
	chunk.Slice(state.new_groups, new_group_count);

	return new_group_count;
}

SinkResultType PhysicalRecursiveCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<RecursiveCTEState>();
	if (!union_all) {
		idx_t match_count = ProbeHT(chunk, gstate);
		if (match_count > 0) {
			gstate.intermediate_table.Append(chunk);
		}
	} else {
		gstate.intermediate_table.Append(chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalRecursiveCTE::GetData(ExecutionContext &context, DataChunk &chunk,
                                               OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<RecursiveCTEState>();
	if (!gstate.initialized) {
		gstate.intermediate_table.InitializeScan(gstate.scan_state);
		gstate.finished_scan = false;
		gstate.initialized = true;
	}
	while (chunk.size() == 0) {
		if (!gstate.finished_scan) {
			// scan any chunks we have collected so far
			gstate.intermediate_table.Scan(gstate.scan_state, chunk);
			if (chunk.size() == 0) {
				gstate.finished_scan = true;
			} else {
				break;
			}
		} else {
			// we have run out of chunks
			// now we need to recurse
			// we set up the working table as the data we gathered in this iteration of the recursion
			working_table->Reset();
			working_table->Combine(gstate.intermediate_table);
			// and we clear the intermediate table
			gstate.finished_scan = false;
			gstate.intermediate_table.Reset();
			// now we need to re-execute all of the pipelines that depend on the recursion
			ExecuteRecursivePipelines(context);

			// check if we obtained any results
			// if not, we are done
			if (gstate.intermediate_table.Count() == 0) {
				gstate.finished_scan = true;
				break;
			}
			// set up the scan again
			gstate.intermediate_table.InitializeScan(gstate.scan_state);
		}
	}

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

void PhysicalRecursiveCTE::ExecuteRecursivePipelines(ExecutionContext &context) const {
	if (!recursive_meta_pipeline) {
		throw InternalException("Missing meta pipeline for recursive CTE");
	}
	D_ASSERT(recursive_meta_pipeline->HasRecursiveCTE());

	// get and reset pipelines
	vector<shared_ptr<Pipeline>> pipelines;
	recursive_meta_pipeline->GetPipelines(pipelines, true);
	for (auto &pipeline : pipelines) {
		auto sink = pipeline->GetSink();
		if (sink.get() != this) {
			sink->sink_state.reset();
		}
		for (auto &op_ref : pipeline->GetOperators()) {
			auto &op = op_ref.get();
			op.op_state.reset();
		}
		pipeline->ClearSource();
	}

	// get the MetaPipelines in the recursive_meta_pipeline and reschedule them
	vector<shared_ptr<MetaPipeline>> meta_pipelines;
	recursive_meta_pipeline->GetMetaPipelines(meta_pipelines, true, false);
	auto &executor = recursive_meta_pipeline->GetExecutor();
	vector<shared_ptr<Event>> events;
	executor.ReschedulePipelines(meta_pipelines, events);

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
void PhysicalRecursiveCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();
	recursive_meta_pipeline.reset();

	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);

	auto &executor = meta_pipeline.GetExecutor();
	executor.AddRecursiveCTE(*this);

	// the LHS of the recursive CTE is our initial state
	auto &initial_state_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	initial_state_pipeline.Build(*children[0]);

	// the RHS is the recursive pipeline
	recursive_meta_pipeline = make_shared<MetaPipeline>(executor, state, this);
	recursive_meta_pipeline->SetRecursiveCTE();
	recursive_meta_pipeline->Build(*children[1]);
}

vector<const_reference<PhysicalOperator>> PhysicalRecursiveCTE::GetSources() const {
	return {*this};
}

string PhysicalRecursiveCTE::ParamsToString() const {
	string result = "";
	result += "\n[INFOSEPARATOR]\n";
	result += ctename;
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("idx: %llu", table_index);
	return result;
}

} // namespace duckdb
