#include "duckdb/execution/operator/set/physical_cte.hpp"

#include "duckdb/common/types/column/column_data_collection.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/pipeline_broadcast_exchange.hpp"

namespace duckdb {

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
		if (unregistered) {
			return;
		}
		unregistered = true;
		exchange->UnregisterConsumer(consumer_idx);
	}

	shared_ptr<PipelineBroadcastExchange> exchange;
	idx_t consumer_idx;
	bool unregistered = false;
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
	result["CTE Mode"] = direct_fanout ? "DIRECT_FANOUT" : "STREAMING_FANOUT";
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
	    : op(op), working_table_ref(op.working_table.get()), exchange(op.exchange) {
		ResetState(context);
	}
	const PhysicalCTE &op;
	optional_ptr<ColumnDataCollection> working_table_ref;
	shared_ptr<PipelineBroadcastExchange> exchange;

	mutex lhs_lock;

private:
	void ResetState(ClientContext &context) {
		if (exchange) {
			exchange->Reset();
			working_table_ref = nullptr;
		} else {
			op.working_table->Reset();
			working_table_ref = op.working_table.get();
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
		lock_guard<mutex> guard(lhs_lock);
		working_table_ref->Combine(input);
	}
};

class CTELocalState : public LocalSinkState {
public:
	explicit CTELocalState(ClientContext &context, const PhysicalCTE &op) : materialized_mode(!op.exchange) {
		if (materialized_mode) {
			lhs_data = make_uniq<ColumnDataCollection>(context, op.working_table->Types());
			lhs_data->InitializeAppend(append_state);
		} else {
			exchange_state = op.exchange->GetLocalState(context);
		}
	}

	unique_ptr<LocalSinkState> distinct_state;
	unique_ptr<ColumnDataCollection> lhs_data;
	ColumnDataAppendState append_state;
	bool materialized_mode;
	unique_ptr<PipelineBroadcastExchangeLocalState> exchange_state;

	void Append(DataChunk &input) {
		D_ASSERT(materialized_mode);
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
	if (exchange) {
		D_ASSERT(gstate.exchange);
		D_ASSERT(lstate.exchange_state);
		return gstate.exchange->Push(chunk, *lstate.exchange_state, input.interrupt_state);
	}
	lstate.Append(chunk);

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCTE::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	auto &lstate = input.local_state.Cast<CTELocalState>();
	if (exchange) {
		D_ASSERT(lstate.exchange_state);
		return exchange->FinishLocal(*lstate.exchange_state, input.interrupt_state);
	}
	auto &gstate = input.global_state.Cast<CTEGlobalState>();
	D_ASSERT(lstate.lhs_data);
	gstate.MergeIT(*lstate.lhs_data);

	return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCTE::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                       OperatorSinkFinalizeInput &input) const {
	if (exchange) {
		auto &gstate = input.global_state.Cast<CTEGlobalState>();
		gstate.exchange->Finish();
		gstate.exchange->FinishDirectConsumers();
	}
	return SinkFinalizeType::READY;
}

//===--------------------------------------------------------------------===//
// Pipeline Construction
//===--------------------------------------------------------------------===//
void PhysicalCTE::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	D_ASSERT(children.size() == 2);
	op_state.reset();
	sink_state.reset();

	auto &state = meta_pipeline.GetState();

	auto &child_meta_pipeline =
	    meta_pipeline.CreateChildMetaPipeline(current, *this, MetaPipelineType::REGULAR, !exchange);
	child_meta_pipeline.Build(children[0]);

	for (auto &cte_scan : cte_scans) {
		state.cte_dependencies.insert(make_pair(cte_scan, reference<Pipeline>(*child_meta_pipeline.GetBasePipeline())));
	}

	// If the CTE body is a DML statement (INSERT/UPDATE/DELETE/MERGE INTO), all MetaPipelines
	// created while building children[1] (the query side) must run after the DML completes.
	// We follow the same pattern as PhysicalJoin::BuildJoinPipelines: capture the DML pipelines
	// and the current last child before building children[1], then call AddRecursiveDependencies
	// with force=true so that ordering is always enforced (not just when pipelines exceed the
	// thread count, as is the case for join build dependencies).
	vector<shared_ptr<Pipeline>> dml_pipelines;
	optional_ptr<MetaPipeline> last_child_ptr;
	if (cte_body_is_dml) {
		child_meta_pipeline.GetPipelines(dml_pipelines, false);
		last_child_ptr = meta_pipeline.GetLastChild();
	}

	children[1].get().BuildPipelines(current, meta_pipeline);

	if (exchange && last_child_ptr && !current.HasDataflowDependencies()) {
		for (auto &dml_pipeline : dml_pipelines) {
			current.AddDependency(dml_pipeline);
		}
	}
	if (last_child_ptr) {
		meta_pipeline.AddRecursiveDependencies(dml_pipelines, *last_child_ptr, true, !!exchange);
	}
}

bool PhysicalCTE::TryRegisterDirectConsumer(Pipeline &pipeline, idx_t consumer_idx) {
	if (!exchange) {
		return false;
	}
	return exchange->TryRegisterDirectConsumer(pipeline, consumer_idx);
}

vector<const_reference<PhysicalOperator>> PhysicalCTE::GetSources() const {
	return children[1].get().GetSources();
}

InsertionOrderPreservingMap<string> PhysicalCTE::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["CTE Name"] = ctename.GetIdentifierName();
	result["Table Index"] = StringUtil::Format("%llu", table_index.index);
	result["Execution Mode"] = exchange ? "STREAMING_FANOUT" : "MATERIALIZED";
	if (exchange && exchange->RunToCompletion()) {
		result["Run To Completion"] = "true";
	}
	if (exchange) {
		auto direct_count = exchange->DirectConsumerCount();
		auto consumer_count = exchange->ConsumerCount();
		D_ASSERT(direct_count <= consumer_count);
		auto buffered_count = consumer_count - direct_count;
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
	SetEstimatedCardinality(result, estimated_cardinality);
	return result;
}

ProgressData PhysicalCTE::GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
                                          const ProgressData source_progress) const {
	auto &state = gstate.Cast<CTEGlobalState>();
	if (state.exchange) {
		return state.exchange->SinkProgress(source_progress, estimated_cardinality);
	}
	lock_guard<mutex> guard(state.lhs_lock);
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
