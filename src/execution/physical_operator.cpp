#include "duckdb/execution/physical_operator.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
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

OperatorFinalizeResultType PhysicalOperator::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                          GlobalOperatorState &gstate, OperatorState &state) const {
	throw InternalException("Calling FinalExecute on a node that is not an operator!");
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
void PhysicalOperator::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	if (IsSink()) {
		// operator is a sink, build a pipeline
		sink_state.reset();
		D_ASSERT(children.size() == 1);

		// single operator: the operator becomes the data source of the current pipeline
		state.SetPipelineSource(current, this);

		// we create a new pipeline starting from the child
		auto child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, this);
		child_meta_pipeline->Build(children[0].get());
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
			children[0]->BuildPipelines(current, meta_pipeline);
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

bool PhysicalOperator::AllOperatorsPreserveOrder() const {
	if (!IsOrderPreserving()) {
		return false;
	}
	for (auto &child : children) {
		if (!child->AllOperatorsPreserveOrder()) {
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

bool CachingPhysicalOperator::CanCacheType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
		return false;
	case LogicalTypeId::STRUCT: {
		auto &entries = StructType::GetChildTypes(type);
		for (auto &entry : entries) {
			if (!CanCacheType(entry.second)) {
				return false;
			}
		}
		return true;
	}
	default:
		return true;
	}
}

CachingPhysicalOperator::CachingPhysicalOperator(PhysicalOperatorType type, vector<LogicalType> types_p,
                                                 idx_t estimated_cardinality)
    : PhysicalOperator(type, move(types_p), estimated_cardinality) {

	caching_supported = true;
	for (auto &col_type : types) {
		if (!CanCacheType(col_type)) {
			caching_supported = false;
			break;
		}
	}
}

OperatorResultType CachingPhysicalOperator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                    GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &state = (CachingOperatorState &)state_p;

	// Execute child operator
	auto child_result = ExecuteInternal(context, input, chunk, gstate, state);

#if STANDARD_VECTOR_SIZE >= 128
	if (!context.pipeline || !caching_supported) {
		return child_result;
	}

	if (context.pipeline->GetSink() && context.pipeline->GetSink()->RequiresBatchIndex() &&
	    context.pipeline->GetSource()->SupportsBatchIndex()) {
		return child_result;
	}

	if (context.pipeline->IsOrderDependent()) {
		return child_result;
	}

	if (chunk.size() < CACHE_THRESHOLD) {
		// we have filtered out a significant amount of tuples
		// add this chunk to the cache and continue

		if (!state.cached_chunk) {
			state.cached_chunk = make_unique<DataChunk>();
			state.cached_chunk->Initialize(Allocator::Get(context.client), chunk.GetTypes());
		}

		state.cached_chunk->Append(chunk);

		if (state.cached_chunk->size() >= (STANDARD_VECTOR_SIZE - CACHE_THRESHOLD) ||
		    child_result == OperatorResultType::FINISHED) {
			// chunk cache full: return it
			chunk.Move(*state.cached_chunk);
			state.cached_chunk->Initialize(Allocator::Get(context.client), chunk.GetTypes());
			return child_result;
		} else {
			// chunk cache not full return empty result
			chunk.Reset();
		}
	}
#endif

	return child_result;
}

OperatorFinalizeResultType CachingPhysicalOperator::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                                 GlobalOperatorState &gstate,
                                                                 OperatorState &state_p) const {
	auto &state = (CachingOperatorState &)state_p;
	if (state.cached_chunk) {
		chunk.Move(*state.cached_chunk);
		state.cached_chunk.reset();
	} else {
		chunk.SetCardinality(0);
	}
	return OperatorFinalizeResultType::FINISHED;
}

} // namespace duckdb
