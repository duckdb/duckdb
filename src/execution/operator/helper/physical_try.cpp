#include "duckdb/execution/operator/helper/physical_try.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/common/vector/constant_vector.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

//===--------------------------------------------------------------------===//
// PhysicalTryCollector
//===--------------------------------------------------------------------===//
PhysicalTryCollector::PhysicalTryCollector(PhysicalPlan &physical_plan, PhysicalOperator &child,
                                           idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TRY_COLLECTOR, child.GetTypes(), estimated_cardinality) {
	children.push_back(child);
}

class TryCollectorGlobalState : public GlobalSinkState {
public:
	TryCollectorGlobalState(ClientContext &context, const vector<LogicalType> &types) : collection(context, types) {
	}

	ColumnDataCollection collection;
};

unique_ptr<GlobalSinkState> PhysicalTryCollector::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TryCollectorGlobalState>(context, types);
}

SinkResultType PhysicalTryCollector::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<TryCollectorGlobalState>();
	gstate.collection.Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

ColumnDataCollection &PhysicalTryCollector::Collection() const {
	D_ASSERT(sink_state);
	return sink_state->Cast<TryCollectorGlobalState>().collection;
}

SourceResultType PhysicalTryCollector::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                       OperatorSourceInput &input) const {
	return SourceResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// PhysicalTry
//===--------------------------------------------------------------------===//
PhysicalTry::PhysicalTry(PhysicalPlan &physical_plan, PhysicalOperator &child, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TRY, child.GetTypes(), estimated_cardinality) {
	children.push_back(child);
}

class TryGlobalSourceState : public GlobalSourceState {
public:
	//! Whether the child plan has been executed
	bool executed = false;
	//! Whether the child plan raised an execution error
	bool errored = false;
	//! Whether the NULL row of an errored boundary has been emitted
	bool null_row_emitted = false;
	//! Scan over the collected result of a successful child plan
	bool scan_initialized = false;
	ColumnDataScanState scan_state;
};

unique_ptr<GlobalSourceState> PhysicalTry::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<TryGlobalSourceState>();
}

//! Run the child plan to completion in a nested executor. Errors raised by the child plan surface here instead of
//! failing the outer query through its executor.
static void ExecuteTryPlan(ClientContext &client, PhysicalOperator &plan) {
	Executor nested_executor(client, true);
	try {
		nested_executor.Initialize(plan);
		while (true) {
			if (client.IsInterrupted() && !nested_executor.HasError()) {
				// the outer query was interrupted while this thread was driving the nested plan
				throw InterruptException();
			}
			auto result = nested_executor.ExecuteTask();
			if (result == QueryResultState::FINISHED) {
				break;
			}
			if (result == QueryResultState::BLOCKED || result == QueryResultState::NO_TASKS_AVAILABLE) {
				if (!nested_executor.HasTaskInProgress()) {
					nested_executor.WaitForTask();
				}
			}
		}
	} catch (...) {
		// drain before unwinding so that no task of the nested executor outlives it
		nested_executor.CancelTasks();
		throw;
	}
	// a worker thread may still hold the last reference to a finished task, whose destructor touches the executor
	// CancelTasks waits until every task object of this executor has been released
	nested_executor.CancelTasks();
}

SourceResultType PhysicalTry::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                              OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<TryGlobalSourceState>();
	auto &collector = children[0].get();
	if (!gstate.executed) {
		gstate.executed = true;
		try {
			ExecuteTryPlan(context.client, collector);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (!Exception::IsExecutionError(error.Type())) {
				throw;
			}
			gstate.errored = true;
		}
	}
	if (gstate.errored) {
		// an execution error occurred inside the boundary: the scalar subquery result is a single NULL row
		if (gstate.null_row_emitted) {
			return SourceResultType::FINISHED;
		}
		gstate.null_row_emitted = true;
		for (auto &vec : chunk.data) {
			vec.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(vec, true);
		}
		chunk.SetCardinality(1);
		return SourceResultType::HAVE_MORE_OUTPUT;
	}
	auto &collection = collector.Cast<PhysicalTryCollector>().Collection();
	if (!gstate.scan_initialized) {
		collection.InitializeScan(gstate.scan_state);
		gstate.scan_initialized = true;
	}
	collection.Scan(gstate.scan_state, chunk);
	return chunk.size() == 0 ? SourceResultType::FINISHED : SourceResultType::HAVE_MORE_OUTPUT;
}

void PhysicalTry::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	auto &state = meta_pipeline.GetState();
	state.SetPipelineSource(current, *this);
	// the child plan is deliberately not built into the pipelines of the outer query
	// it is executed by a nested executor created in GetData, so that its errors can be contained
}

} // namespace duckdb
