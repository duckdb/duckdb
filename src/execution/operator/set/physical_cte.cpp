#include "duckdb/execution/operator/set/physical_cte.hpp"

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

PhysicalCTE::PhysicalCTE(string ctename, idx_t table_index, vector<LogicalType> types, unique_ptr<PhysicalOperator> top,
                         unique_ptr<PhysicalOperator> bottom, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::CTE, std::move(types), estimated_cardinality), table_index(table_index),
      ctename(std::move(ctename)) {
	children.push_back(std::move(top));
	children.push_back(std::move(bottom));
}

PhysicalCTE::~PhysicalCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CTEState : public GlobalSinkState {
public:
	explicit CTEState(ClientContext &context, const PhysicalCTE &op)
	    : intermediate_table(context, op.children[1]->GetTypes()) {
	}
	ColumnDataCollection intermediate_table;
	ColumnDataScanState scan_state;
	bool initialized = false;
	bool finished_scan = false;
};

unique_ptr<GlobalSinkState> PhysicalCTE::GetGlobalSinkState(ClientContext &context) const {
	working_table->Reset();
	return make_uniq<CTEState>(context, *this);
}

SinkResultType PhysicalCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CTEState>();
	if (!gstate.finished_scan) {
		working_table->Append(chunk);
	} else {
		gstate.intermediate_table.Append(chunk);
	}
	return SinkResultType::NEED_MORE_INPUT;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
SourceResultType PhysicalCTE::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<CTEState>();
	if (!gstate.initialized) {
		gstate.intermediate_table.InitializeScan(gstate.scan_state);
		gstate.finished_scan = false;
		gstate.initialized = true;
	}
	if (!gstate.finished_scan) {
		gstate.finished_scan = true;
		ExecuteRecursivePipelines(context);
	}

	gstate.intermediate_table.Scan(gstate.scan_state, chunk);

	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

void PhysicalCTE::ExecuteRecursivePipelines(ExecutionContext &context) const {
	if (!recursive_meta_pipeline) {
		throw InternalException("Missing meta pipeline for recursive CTE");
	}

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
void PhysicalCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	op_state.reset();
	sink_state.reset();
	recursive_meta_pipeline.reset();

	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);

	auto &executor = meta_pipeline.GetExecutor();
	executor.AddMaterializedCTE(*this);

	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
	child_meta_pipeline.Build(*children[0]);

	// the RHS is the recursive pipeline
	recursive_meta_pipeline = make_shared<MetaPipeline>(executor, state, this);
	if (meta_pipeline.HasRecursiveCTE()) {
		recursive_meta_pipeline->SetRecursiveCTE();
	}
	recursive_meta_pipeline->Build(*children[1]);
}

vector<const_reference<PhysicalOperator>> PhysicalCTE::GetSources() const {
	return {*this};
}

string PhysicalCTE::ParamsToString() const {
	string result = "";
	result += "\n[INFOSEPARATOR]\n";
	result += ctename;
	result += "\n[INFOSEPARATOR]\n";
	result += StringUtil::Format("idx: %llu", table_index);
	return result;
}

} // namespace duckdb
