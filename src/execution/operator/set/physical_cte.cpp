#include "duckdb/execution/operator/set/physical_cte.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_broadcast_exchange.hpp"

namespace duckdb {

enum class CTESinkExecutionState : uint8_t { ACTIVE, BLOCKED, STOPPED };
enum class CTECombineState : uint8_t { PENDING, COMBINED };

class CTEConsumerGlobalSourceState : public GlobalSourceState {
public:
	CTEConsumerGlobalSourceState(shared_ptr<PipelineBroadcastExchange> exchange_p, idx_t consumer_idx_p)
	    : exchange(std::move(exchange_p)), consumer_idx(consumer_idx_p) {
	}

	~CTEConsumerGlobalSourceState() override {
		Unregister();
	}

	idx_t MaxThreads() override {
		return exchange->MaxThreads();
	}

	void Unregister() {
		if (unregistered.exchange(true)) {
			return;
		}
		exchange->UnregisterConsumer(consumer_idx);
	}

	shared_ptr<PipelineBroadcastExchange> exchange;
	idx_t consumer_idx;
	atomic<bool> unregistered {false};
};

class CTEConsumerLocalSourceState : public LocalSourceState {
public:
	shared_ptr<DataChunk> current_chunk;
};

PhysicalCTEConsumerSource::PhysicalCTEConsumerSource(PhysicalPlan &physical_plan, vector<LogicalType> types,
                                                     idx_t estimated_cardinality, TableIndex cte_index,
                                                     shared_ptr<PipelineBroadcastExchange> exchange, idx_t consumer_idx)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CTE_SCAN, std::move(types), estimated_cardinality),
      cte_index(cte_index), exchange(std::move(exchange)), consumer_idx(consumer_idx) {
}

unique_ptr<GlobalSourceState> PhysicalCTEConsumerSource::GetGlobalSourceState(ClientContext &context) const {
	return make_uniq<CTEConsumerGlobalSourceState>(exchange, consumer_idx);
}

unique_ptr<LocalSourceState> PhysicalCTEConsumerSource::GetLocalSourceState(ExecutionContext &context,
                                                                            GlobalSourceState &gstate) const {
	return make_uniq<CTEConsumerLocalSourceState>();
}

SourceResultType PhysicalCTEConsumerSource::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                            OperatorSourceInput &input) const {
	auto &gstate = input.global_state.Cast<CTEConsumerGlobalSourceState>();
	auto &lstate = input.local_state.Cast<CTEConsumerLocalSourceState>();
	return gstate.exchange->Scan(gstate.consumer_idx, chunk, lstate.current_chunk, input.interrupt_state);
}

ProgressData PhysicalCTEConsumerSource::GetProgress(ClientContext &context, GlobalSourceState &gstate) const {
	auto &state = gstate.Cast<CTEConsumerGlobalSourceState>();
	return state.exchange->ScanProgress(state.consumer_idx, estimated_cardinality);
}

void PhysicalCTEConsumerSource::SourceFinished(ClientContext &context, GlobalSourceState &gstate) const {
	gstate.Cast<CTEConsumerGlobalSourceState>().Unregister();
}

InsertionOrderPreservingMap<string> PhysicalCTEConsumerSource::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Index"] = StringUtil::Format("%llu", cte_index.index);
	result["CTE Mode"] = EnumUtil::ToString(exchange->GetConsumerMode(consumer_idx));
	result["Consumer"] = StringUtil::Format("%llu", consumer_idx);
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

PhysicalCTE::PhysicalCTE(PhysicalPlan &physical_plan, Identifier ctename, TableIndex table_index,
                         vector<LogicalType> types, PhysicalOperator &top, PhysicalOperator &bottom,
                         idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::CTE, std::move(types), estimated_cardinality),
      table_index(table_index), ctename(std::move(ctename)) {
	children.push_back(top);
	children.push_back(bottom);
}

PhysicalCTE::~PhysicalCTE() {
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//
class CTEGlobalState : public GlobalSinkState {
public:
	explicit CTEGlobalState(ClientContext &context, const PhysicalCTE &op)
	    : op(op), working_table_ref(op.working_table.get()),
	      exchange(op.UseStreamingExchange() ? op.exchange : nullptr), execution_mode(op.GetExecutionMode()) {
		ResetState(context);
	}

	~CTEGlobalState() override {
		if (exchange) {
			exchange->Cancel();
		}
	}
	const PhysicalCTE &op;
	optional_ptr<ColumnDataCollection> working_table_ref;
	shared_ptr<PipelineBroadcastExchange> exchange;
	CTEExecutionMode execution_mode;

	annotated_mutex lhs_lock;

private:
	void ResetState(ClientContext &context) {
		if (exchange) {
			exchange->Reset();
		}
		if (execution_mode != CTEExecutionMode::STREAMING_FANOUT) {
			D_ASSERT(op.working_table);
			op.working_table->Reset();
			working_table_ref = op.working_table.get();
		} else {
			working_table_ref = nullptr;
		}
		GlobalSinkState::Reset(context);
	}

public:
	bool SupportsReuse() const override {
		return true;
	}

	void Reset(ClientContext &context) override {
		ResetState(context);
	}

	void MergeIT(ColumnDataCollection &input) {
		annotated_lock_guard<annotated_mutex> guard(lhs_lock);
		working_table_ref->Combine(input);
	}
};

class CTELocalState : public LocalSinkState {
public:
	explicit CTELocalState(ClientContext &context, const PhysicalCTE &op) : execution_mode(op.GetExecutionMode()) {
		if (execution_mode != CTEExecutionMode::STREAMING_FANOUT) {
			D_ASSERT(op.working_table);
			lhs_data = make_uniq<ColumnDataCollection>(context, op.working_table->Types());
			lhs_data->InitializeAppend(append_state);
		}
		if (execution_mode != CTEExecutionMode::MATERIALIZED) {
			D_ASSERT(op.exchange);
			exchange_state = op.exchange->GetLocalState(context);
		}
	}

	unique_ptr<LocalSinkState> distinct_state;
	unique_ptr<ColumnDataCollection> lhs_data;
	ColumnDataAppendState append_state;
	CTEExecutionMode execution_mode;
	CTESinkExecutionState sink_execution_state = CTESinkExecutionState::ACTIVE;
	// Combine can be retried after a blocked exchange finish.
	CTECombineState combine_state = CTECombineState::PENDING;
	unique_ptr<PipelineBroadcastExchangeLocalState> exchange_state;

	void Append(DataChunk &input) {
		D_ASSERT(execution_mode != CTEExecutionMode::STREAMING_FANOUT);
		D_ASSERT(lhs_data);
		lhs_data->Append(append_state, input);
	}
};

unique_ptr<GlobalSinkState> PhysicalCTE::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CTEGlobalState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCTE::GetLocalSinkState(ExecutionContext &context) const {
	auto state = make_uniq<CTELocalState>(context.client, *this);
	return std::move(state);
}

SinkResultType PhysicalCTE::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<CTEGlobalState>();
	auto &lstate = input.local_state.Cast<CTELocalState>();
	SinkResultType result = SinkResultType::NEED_MORE_INPUT;
	if (lstate.execution_mode != CTEExecutionMode::MATERIALIZED) {
		D_ASSERT(gstate.exchange);
		D_ASSERT(lstate.exchange_state);
		result = gstate.exchange->Push(chunk, *lstate.exchange_state, input.interrupt_state);
		if (result == SinkResultType::BLOCKED) {
			if (lstate.sink_execution_state != CTESinkExecutionState::BLOCKED) {
				DUCKDB_LOG(context.client, PhysicalOperatorLogType, *this, "PhysicalCTE", "ExchangeBlocked",
				           {{"mode", EnumUtil::ToString(lstate.execution_mode)}});
				lstate.sink_execution_state = CTESinkExecutionState::BLOCKED;
			}
			return result;
		}
		if (result == SinkResultType::FINISHED && lstate.sink_execution_state != CTESinkExecutionState::STOPPED) {
			DUCKDB_LOG(context.client, PhysicalOperatorLogType, *this, "PhysicalCTE", "ProducerStoppedEarly",
			           {{"mode", EnumUtil::ToString(lstate.execution_mode)}});
			lstate.sink_execution_state = CTESinkExecutionState::STOPPED;
		} else if (result != SinkResultType::FINISHED) {
			lstate.sink_execution_state = CTESinkExecutionState::ACTIVE;
		}
	}
	if (lstate.execution_mode != CTEExecutionMode::STREAMING_FANOUT) {
		// In hybrid mode, append only after a successful exchange push; blocked pushes retry the same chunk.
		lstate.Append(chunk);
		return SinkResultType::NEED_MORE_INPUT;
	}

	return result;
}

SinkCombineResultType PhysicalCTE::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<CTELocalState>();
	if (lstate.execution_mode != CTEExecutionMode::MATERIALIZED) {
		D_ASSERT(lstate.exchange_state);
		auto result = exchange->FinishLocal(*lstate.exchange_state, input.interrupt_state);
		if (result == SinkCombineResultType::BLOCKED) {
			return result;
		}
	}
	if (lstate.execution_mode != CTEExecutionMode::STREAMING_FANOUT &&
	    lstate.combine_state == CTECombineState::PENDING) {
		auto &gstate = input.global_state.Cast<CTEGlobalState>();
		D_ASSERT(lstate.lhs_data);
		gstate.MergeIT(*lstate.lhs_data);
		lstate.combine_state = CTECombineState::COMBINED;
	}

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCTE::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	if (exchange && UseStreamingExchange()) {
		auto &gstate = input.global_state.Cast<CTEGlobalState>();
		gstate.exchange->Finish();
		gstate.exchange->FinishDirectConsumers();
		DUCKDB_LOG(context, PhysicalOperatorLogType, *this, "PhysicalCTE", "ProducerFinished",
		           {{"mode", EnumUtil::ToString(gstate.execution_mode)}});
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	pipeline_selection_state = CTEPipelineSelectionState::UNRESOLVED;
	op_state.reset();
	sink_state.reset();
	if (exchange) {
		exchange->SetLogOperator(*this);
		// Prepared plans rebuild pipelines while keeping the physical CTE and consumer indexes.
		exchange->ResetConsumerRegistrations();
	}

	auto &state = meta_pipeline.GetState();

	auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(
	    current, *this, MetaPipelineType::REGULAR,
	    exchange ? MetaPipelineDependencyMode::NO_DEPENDENCY : MetaPipelineDependencyMode::ADD_DEPENDENCY);
	child_meta_pipeline.Build(children[0]);

	for (auto &cte_scan : cte_scans) {
		state.cte_dependencies.insert(make_pair(cte_scan, reference<Pipeline>(*child_meta_pipeline.GetBasePipeline())));
	}

	// If the CTE body is a DML statement (INSERT/UPDATE/DELETE/MERGE INTO), all MetaPipelines
	// created while building children[1] (the query side) must run after the DML completes.
	// We follow the same pattern as PhysicalJoin::BuildJoinPipelines: capture the DML pipelines
	// and the current last child before building children[1], then call AddRecursiveDependencies
	// with RecursiveDependencyMode::FORCE so ordering is always enforced (not just when pipelines exceed the
	// thread count, as is the case for join build dependencies).
	vector<shared_ptr<Pipeline>> dml_pipelines;
	optional_ptr<MetaPipeline> last_child_ptr;
	if (cte_body_is_dml) {
		child_meta_pipeline.GetPipelines(dml_pipelines, false);
		last_child_ptr = meta_pipeline.GetLastChild();
	}

	children[1].get().BuildPipelines(current, meta_pipeline);
	pipeline_selection_state = CTEPipelineSelectionState::RESOLVED;

	if (exchange && !UseStreamingExchange()) {
		// All exchange consumers were converted to materialized scans during pipeline construction.
		auto cte_pipeline = child_meta_pipeline.GetBasePipeline();
		current.AddDependency(cte_pipeline);
	}
	if (exchange && last_child_ptr && !current.HasDataflowDependencies()) {
		for (auto &dml_pipeline : dml_pipelines) {
			current.AddDependency(dml_pipeline);
		}
	}
	if (last_child_ptr) {
		meta_pipeline.AddRecursiveDependencies(dml_pipelines, *last_child_ptr, RecursiveDependencyMode::FORCE,
		                                       exchange ? DataflowDependencyMode::SKIP
		                                                : DataflowDependencyMode::INCLUDE);
	}
}

bool PhysicalCTE::TryRegisterDirectConsumer(Pipeline &pipeline, idx_t consumer_idx) {
	if (!exchange) {
		return false;
	}
	return exchange->TryRegisterDirectConsumer(pipeline, consumer_idx);
}

bool PhysicalCTE::ShouldUseBufferedConsumer(Pipeline &pipeline) const {
	if (!exchange) {
		return false;
	}
	if (exchange->RegisteredConsumerCount() == 1) {
		return true;
	}
	// Multi-consumer all-data fanout is cheaper through the materialized table. Keep buffered exchange only
	// for consumers that can make streaming observable by stopping before source exhaustion.
	return pipeline.CanStopSourceEarly();
}

void PhysicalCTE::RegisterBufferedConsumer(idx_t consumer_idx) {
	D_ASSERT(exchange);
	exchange->SelectBufferedConsumer(consumer_idx);
}

void PhysicalCTE::RegisterMaterializedConsumer(idx_t consumer_idx) {
	D_ASSERT(exchange);
	// Materialized consumers must not keep the exchange producer active or force exchange buffering.
	exchange->SelectMaterializedConsumer(consumer_idx);
}

CTEExecutionMode PhysicalCTE::GetExecutionMode() const {
	if (!exchange) {
		return CTEExecutionMode::MATERIALIZED;
	}
	auto summary = exchange->GetConsumerSummary();
	if (summary.ExchangeConsumerCount() == 0) {
		return CTEExecutionMode::MATERIALIZED;
	}
	if (summary.materialized > 0) {
		return CTEExecutionMode::HYBRID_FANOUT;
	}
	return CTEExecutionMode::STREAMING_FANOUT;
}

bool PhysicalCTE::UseStreamingExchange() const {
	return GetExecutionMode() != CTEExecutionMode::MATERIALIZED;
}

vector<const_reference<PhysicalOperator>> PhysicalCTE::GetSources() const {
	return children[1].get().GetSources();
}

InsertionOrderPreservingMap<string> PhysicalCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename.GetIdentifierName();
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	if (exchange && pipeline_selection_state == CTEPipelineSelectionState::UNRESOLVED) {
		result["Execution Mode"] = "PIPELINE_DEPENDENT";
		if (exchange->RunToCompletion()) {
			result["Run To Completion"] = "true";
		}
		SetEstimatedCardinality(result, estimated_cardinality);
		return result;
	}
	auto execution_mode = GetExecutionMode();
	result["Execution Mode"] = EnumUtil::ToString(execution_mode);
	if (UseStreamingExchange() && exchange->RunToCompletion()) {
		result["Run To Completion"] = "true";
	}
	if (UseStreamingExchange()) {
		auto summary = exchange->GetConsumerSummary();
		auto direct_count = summary.direct;
		auto consumer_count = summary.ExchangeConsumerCount();
		D_ASSERT(direct_count <= consumer_count);
		auto buffered_count = summary.buffered;
		if (direct_count == 0) {
			result["Fanout Mode"] = "BUFFERED";
		} else if (direct_count == consumer_count) {
			result["Fanout Mode"] = "DIRECT";
		} else {
			result["Fanout Mode"] = "MIXED";
		}
		result["Consumers"] = StringUtil::Format("%llu", consumer_count);
		result["Direct Consumers"] = StringUtil::Format("%llu", direct_count);
		result["Buffered Consumers"] = StringUtil::Format("%llu", buffered_count);
		result["Chunk Storage"] = buffered_count == 0 ? "NONE" : "POOLED/COLUMN_DATA";
	}
	if (exchange) {
		auto summary = exchange->GetConsumerSummary();
		if (summary.materialized > 0) {
			result["Materialized Consumers"] = StringUtil::Format("%llu", summary.materialized);
		}
	}
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

ProgressData PhysicalCTE::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
                                          const ProgressData source_progress) const {
	auto &state = gstate.Cast<CTEGlobalState>();
	if (state.exchange) {
		return state.exchange->SinkProgress(source_progress, estimated_cardinality);
	}
	annotated_lock_guard<annotated_mutex> guard(state.lhs_lock);
	if (!state.working_table_ref) {
		return ProgressData {0, 1, true};
	}
	auto &working_table = *state.working_table_ref;
	auto count = double(working_table.Count());
	ProgressData progress;
	progress.done = count;
	progress.total = count + source_progress.total;
	return progress;
}

} // namespace duckdb
