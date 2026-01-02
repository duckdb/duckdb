#include "duckdb/execution/physical_operator.hpp"

#include "duckdb/common/printer.hpp"
#include "duckdb/common/render_tree.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/tree_renderer.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

PhysicalOperator::PhysicalOperator(PhysicalPlan &physical_plan, PhysicalOperatorType type, vector<LogicalType> types,
                                   idx_t estimated_cardinality)
    : children(physical_plan.ArenaRef()), type(type), types(std::move(types)),
      estimated_cardinality(estimated_cardinality) {
}

string PhysicalOperator::GetName() const {
	return PhysicalOperatorToString(type);
}

string PhysicalOperator::ToString(ExplainFormat format) const {
	auto renderer = TreeRenderer::CreateRenderer(format);
	stringstream ss;
	auto tree = RenderTree::CreateRenderTree(*this);
	renderer->ToStream(*tree, ss);
	return ss.str();
}

// LCOV_EXCL_START
void PhysicalOperator::Print() const {
	Printer::Print(ToString());
}
// LCOV_EXCL_STOP

vector<const_reference<PhysicalOperator>> PhysicalOperator::GetChildren() const {
	vector<const_reference<PhysicalOperator>> result;
	for (auto &child : children) {
		result.push_back(child.get());
	}
	return result;
}

void PhysicalOperator::SetEstimatedCardinality(InsertionOrderPreservingMap<string> &result,
                                               idx_t estimated_cardinality) {
	result[RenderTreeNode::ESTIMATED_CARDINALITY] = StringUtil::Format("%llu", estimated_cardinality);
}

idx_t PhysicalOperator::EstimatedThreadCount() const {
	idx_t result = 0;
	if (children.empty()) {
		// Terminal operator, e.g., base table, these decide the degree of parallelism of pipelines
		result = MaxValue<idx_t>(estimated_cardinality / (DEFAULT_ROW_GROUP_SIZE * 2), 1);
	} else if (type == PhysicalOperatorType::UNION) {
		// We can run union pipelines in parallel, so we sum up the thread count of the children
		for (auto &child : children) {
			result += child.get().EstimatedThreadCount();
		}
	} else {
		// For other operators we take the maximum of the children
		for (auto &child : children) {
			result = MaxValue(child.get().EstimatedThreadCount(), result);
		}
	}
	return result;
}

bool PhysicalOperator::CanSaturateThreads(ClientContext &context) const {
#ifdef DEBUG
	// In debug mode we always return true here so that the code that depends on it is well-tested
	return true;
#else
	const auto num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	return EstimatedThreadCount() >= num_threads;
#endif
}

//===--------------------------------------------------------------------===//
// Operator
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
unique_ptr<OperatorState> PhysicalOperator::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<OperatorState>();
}

unique_ptr<GlobalOperatorState> PhysicalOperator::GetGlobalOperatorState(ClientContext &context) const {
	return make_uniq<GlobalOperatorState>();
}

OperatorResultType PhysicalOperator::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                             GlobalOperatorState &gstate, OperatorState &state) const {
	throw InternalException("Calling Execute on a node that is not an operator!");
}

OperatorFinalizeResultType PhysicalOperator::FinalExecute(ExecutionContext &context, DataChunk &chunk,
                                                          GlobalOperatorState &gstate, OperatorState &state) const {
	throw InternalException("Calling FinalExecute on a node that is not an operator!");
}

OperatorFinalResultType PhysicalOperator::OperatorFinalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                                           OperatorFinalizeInput &input) const {
	throw InternalException("Calling FinalExecute on a node that is not an operator!");
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//
unique_ptr<LocalSourceState> PhysicalOperator::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_uniq<LocalSourceState>();
}

unique_ptr<GlobalSourceState> PhysicalOperator::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<GlobalSourceState>();
}

// LCOV_EXCL_START
SourceResultType PhysicalOperator::GetData(ExecutionContext &context, DataChunk &chunk,
                                           OperatorSourceInput &input) const {
	return GetDataInternal(context, chunk, input);
}

SourceResultType PhysicalOperator::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	throw InternalException("Calling GetDataInternal on a node that is not a source!");
}

OperatorPartitionData PhysicalOperator::GetPartitionData(ExecutionContext &context, DataChunk &chunk,
                                                         GlobalSourceState &gstate, LocalSourceState &lstate,
                                                         const OperatorPartitionInfo &partition_info) const {
	throw InternalException("Calling GetPartitionData on a node that does not support it");
}

ProgressData PhysicalOperator::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	ProgressData res;
	res.SetInvalid();
	return res;
}
// LCOV_EXCL_STOP

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
// LCOV_EXCL_START
SinkResultType PhysicalOperator::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	throw InternalException("Calling Sink on a node that is not a sink!");
}

// LCOV_EXCL_STOP

SinkCombineResultType PhysicalOperator::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	return SinkCombineResultType::FINISHED;
}

void PhysicalOperator::PrepareFinalize(ClientContext &context, GlobalSinkState &sink_state) const {
}

SinkFinalizeType PhysicalOperator::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	return SinkFinalizeType::READY;
}

SinkNextBatchType PhysicalOperator::NextBatch(ExecutionContext &context, OperatorSinkNextBatchInput &input) const {
	return SinkNextBatchType::READY;
}

unique_ptr<LocalSinkState> PhysicalOperator::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<LocalSinkState>();
}

unique_ptr<GlobalSinkState> PhysicalOperator::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<GlobalSinkState>();
}

idx_t PhysicalOperator::GetMaxThreadMemory(ClientContext &context) {
	// Memory usage per thread should scale with max mem / num threads
	// We take 1/4th of this, to be conservative
	auto max_memory = BufferManager::GetBufferManager(context).GetQueryMaxMemory();
	auto num_threads = NumericCast<idx_t>(TaskScheduler::GetScheduler(context).NumberOfThreads());
	return (max_memory / num_threads) / 4;
}

bool PhysicalOperator::OperatorCachingAllowed(ExecutionContext &context) {
	if (!context.client.config.enable_caching_operators) {
		return false;
	} else if (!context.pipeline) {
		return false;
	} else if (!context.pipeline->GetSink()) {
		return false;
	} else if (context.pipeline->IsOrderDependent()) {
		return false;
	} else {
		auto partition_info = context.pipeline->GetSink()->RequiredPartitionInfo();
		if (partition_info.AnyRequired()) {
			return false;
		}
	}

	return true;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalOperator::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	if (!IsSink() && children.empty()) {
		// Operator is a source.
		state.SetPipelineSource(current, *this);
		return;
	}

	if (children.size() != 1) {
		throw InternalException("Operator not supported in BuildPipelines");
	}

	if (IsSink()) {
		// Operator is a sink.
		sink_state.reset();

		// It becomes the data source of the current pipeline.
		state.SetPipelineSource(current, *this);

		// Create a new pipeline starting at the child.
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		child_meta_pipeline.Build(children[0].get());
		return;
	}

	// Recurse into the child.
	state.AddPipelineOperator(current, *this);
	children[0].get().BuildPipelines(current, meta_pipeline);
}

vector<const_reference<PhysicalOperator>> PhysicalOperator::GetSources() const {
	vector<const_reference<PhysicalOperator>> result;
	if (!IsSink() && children.empty()) {
		// Operator is a source.
		result.push_back(*this);
		return result;
	}

	if (children.size() != 1) {
		throw InternalException("Operator not supported in GetSource");
	}

	if (IsSink()) {
		result.push_back(*this);
		return result;
	}

	// Recurse into the child.
	return children[0].get().GetSources();
}

bool PhysicalOperator::AllSourcesSupportBatchIndex() const {
	auto sources = GetSources();
	for (auto &source : sources) {
		if (!source.get().SupportsPartitioning(OperatorPartitionInfo::BatchIndex())) {
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
		child.get().Verify();
	}
#endif
}

bool CachingPhysicalOperator::CanCacheType(const LogicalType &type) {
	switch (type.id()) {
	case LogicalTypeId::LIST:
	case LogicalTypeId::MAP:
	case LogicalTypeId::ARRAY:
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

CachingPhysicalOperator::CachingPhysicalOperator(PhysicalPlan &physical_plan, PhysicalOperatorType type,
                                                 vector<LogicalType> types_p, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, type, std::move(types_p), estimated_cardinality) {
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
	auto &state = state_p.Cast<CachingOperatorState>();

	// Execute child operator
	auto child_result = ExecuteInternal(context, input, chunk, gstate, state);

#if STANDARD_VECTOR_SIZE >= 128
	if (!state.initialized) {
		state.initialized = true;
		state.can_cache_chunk = caching_supported && PhysicalOperator::OperatorCachingAllowed(context);
	}
	if (!state.can_cache_chunk) {
		return child_result;
	}
	// TODO chunk size of 0 should not result in a cache being created!
	if (chunk.size() < CACHE_THRESHOLD) {
		// we have filtered out a significant amount of tuples
		// add this chunk to the cache and continue

		if (!state.cached_chunk) {
			state.cached_chunk = make_uniq<DataChunk>();
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
	auto &state = state_p.Cast<CachingOperatorState>();
	if (state.cached_chunk) {
		chunk.Move(*state.cached_chunk);
		state.cached_chunk.reset();
	} else {
		chunk.SetCardinality(0);
	}
	return OperatorFinalizeResultType::FINISHED;
}

} // namespace duckdb
