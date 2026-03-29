#include "duckdb/execution/operator/persistent/physical_trigger.hpp"

namespace duckdb {

struct TriggerGlobalSinkState : public GlobalSinkState {
	atomic<idx_t> row_count {0};
};

PhysicalTrigger::PhysicalTrigger(PhysicalPlan &physical_plan, vector<TriggerInfo> triggers, idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TRIGGER, {LogicalType::BIGINT}, estimated_cardinality),
      triggers(std::move(triggers)) {
}

unique_ptr<GlobalSinkState> PhysicalTrigger::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TriggerGlobalSinkState>();
}

SinkResultType PhysicalTrigger::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<TriggerGlobalSinkState>();
	// The operator (INSERT/UPDATE/DELETE) outputs a single BIGINT row with the affected row count
	// TODO - This will change when RETURNING is supported (for NEW/OLD, REFERENCING NEW TABLE AS)
	D_ASSERT(chunk.ColumnCount() == 1);
	D_ASSERT(chunk.size() == 1);
	gstate.row_count += NumericCast<idx_t>(chunk.GetValue(0, 0).GetValue<int64_t>());
	return SinkResultType::NEED_MORE_INPUT;
}

SinkFinalizeType PhysicalTrigger::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                           OperatorSinkFinalizeInput &input) const {
	auto &gstate = input.global_state.Cast<TriggerGlobalSinkState>();
	TriggerExecutor::Fire(context, triggers, gstate.row_count);
	return SinkFinalizeType::READY;
}

SourceResultType PhysicalTrigger::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                  OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<TriggerGlobalSinkState>();
	chunk.SetCardinality(1);
	chunk.SetValue(0, 0, Value::BIGINT(NumericCast<int64_t>(gstate.row_count.load())));
	return SourceResultType::FINISHED;
}

} // namespace duckdb
