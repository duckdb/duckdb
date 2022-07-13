#include "duckdb/execution/physical_operator.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

string PhysicalOperator::GetName() const {
	return PhysicalOperatorToString(type);
}

string PhysicalOperator::ToString() const {
	TreeRenderer renderer;
	return renderer.ToString(*this);
}

// LCOV_EXCL_START
void PhysicalOperator::Print() const {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

vector<PhysicalOperator *> PhysicalOperator::GetChildren() const {
	vector<PhysicalOperator *> result;
	for (auto &child : children) {
		result.push_back(child.get());
	}
	return result;
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
unique_ptr<OperatorState> PhysicalOperator::GetOperatorState(ExecutionContext &context) const {
	return make_unique<OperatorState>();
}

unique_ptr<GlobalOperatorState> PhysicalOperator::GetGlobalOperatorState(ClientContext &context) const {
	return make_unique<GlobalOperatorState>();
}

OperatorResultType PhysicalOperator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state) const {
	throw InternalException("Calling Execute on a node that is not an operator!");
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalOperator::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_unique<LocalSourceState>();
}

unique_ptr<GlobalSourceState> PhysicalOperator::GetGlobalSourceState(ClientContext &context) const {
	return make_unique<GlobalSourceState>();
}

// LCOV_EXCL_START
void PhysicalOperator::GetData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                               LocalSourceState &lstate) const {
	throw InternalException("Calling GetData on a node that is not a source!");
}

idx_t PhysicalOperator::GetBatchIndex(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
                                      LocalSourceState &lstate) const {
	throw InternalException("Calling GetBatchIndex on a node that does not support it");
}

double PhysicalOperator::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	return -1;
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
SinkResultType PhysicalOperator::Sink(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate,
                                      DataChunk &input) const {
	throw InternalException("Calling Sink on a node that is not a sink!");
}
// LCOV_EXCL_STOP

void PhysicalOperator::Combine(ExecutionContext &context, GlobalSinkState &gstate, LocalSinkState &lstate) const {
}

SinkFinalizeType PhysicalOperator::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            GlobalSinkState &gstate) const {
	return SinkFinalizeType::READY;
}

unique_ptr<LocalSinkState> PhysicalOperator::GetLocalSinkState(ExecutionContext &context) const {
	return make_unique<LocalSinkState>();
}

unique_ptr<GlobalSinkState> PhysicalOperator::GetGlobalSinkState(ClientContext &context) const {
	return make_unique<GlobalSinkState>();
}

idx_t PhysicalOperator::GetMaxThreadMemory(ClientContext &context) {
	// Memory usage per thread should scale with max mem / num threads
	// We take 1/4th of this, to be conservative
	idx_t max_memory = BufferManager::GetBufferManager(context).GetMaxMemory();
	idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	return (max_memory / num_threads) / 4;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalOperator::AddPipeline(Executor &executor, shared_ptr<Pipeline> pipeline, PipelineBuildState &state) {
	if (!state.recursive_cte) {
		// regular pipeline: schedule it
		state.AddPipeline(executor, move(pipeline));
	} else {
		// CTE pipeline! add it to the CTE pipelines
		auto &cte = (PhysicalRecursiveCTE &)*state.recursive_cte;
		cte.pipelines.push_back(move(pipeline));
	}
}

void PhysicalOperator::BuildChildPipeline(Executor &executor, Pipeline &current, PipelineBuildState &state,
                                          PhysicalOperator *pipeline_child) {
	auto pipeline = make_shared<Pipeline>(executor);
	state.SetPipelineSink(*pipeline, this);
	// the current is dependent on this pipeline to complete
	current.AddDependency(pipeline);
	// recurse into the pipeline child
	pipeline_child->BuildPipelines(executor, *pipeline, state);
	AddPipeline(executor, move(pipeline), state);
}

void PhysicalOperator::BuildPipelines(Executor &executor, Pipeline &current, PipelineBuildState &state) {
	op_state.reset();
	if (IsSink()) {
		// operator is a sink, build a pipeline
		sink_state.reset();

		// single operator:
		// the operator becomes the data source of the current pipeline
		state.SetPipelineSource(current, this);
		// we create a new pipeline starting from the child
		D_ASSERT(children.size() == 1);

		BuildChildPipeline(executor, current, state, children[0].get());
	} else {
		// operator is not a sink! recurse in children
		if (children.empty()) {
			// source
			state.SetPipelineSource(current, this);
		} else {
			if (children.size() != 1) {
				throw InternalException("Operator not supported in BuildPipelines");
			}
			state.AddPipelineOperator(current, this);
			children[0]->BuildPipelines(executor, current, state);
		}
	}
}

vector<const PhysicalOperator *> PhysicalOperator::GetSources() const {
	vector<const PhysicalOperator *> result;
	if (IsSink()) {
		D_ASSERT(children.size() == 1);
		result.push_back(this);
		return result;
	} else {
		if (children.empty()) {
			// source
			result.push_back(this);
			return result;
		} else {
			if (children.size() != 1) {
				throw InternalException("Operator not supported in GetSource");
			}
			return children[0]->GetSources();
		}
	}
}

bool PhysicalOperator::AllSourcesSupportBatchIndex() const {
	auto sources = GetSources();
	for (auto &source : sources) {
		if (!source->SupportsBatchIndex()) {
			return false;
		}
	}
	return true;
}

void PhysicalOperator::Verify() {
#ifdef DEBUG
	auto sources = GetSources();
	D_ASSERT(!sources.empty());
	for (auto &child : children) {
		child->Verify();
	}
#endif
}

} // namespace duckdb
