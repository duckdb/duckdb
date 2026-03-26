#include "duckdb/execution/operator/persistent/physical_trigger.hpp"

#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"

namespace duckdb {

struct TriggerGlobalSinkState : public GlobalSinkState {
	atomic<idx_t> row_count {0};
};

static vector<TriggerInfo> CollectTriggers(ClientContext &context, TableCatalogEntry &table, TriggerTiming timing,
                                           TriggerEventType event_type) {
	vector<TriggerInfo> triggers;
	table.ParentSchema().Scan(context, CatalogType::TRIGGER_ENTRY, [&](CatalogEntry &entry) {
		auto &trigger = entry.Cast<TriggerCatalogEntry>();
		if (trigger.timing != timing || trigger.event_type != event_type) {
			return;
		}
		if (trigger.base_table->table_name != table.name) {
			return;
		}
		D_ASSERT(trigger.sql_body);
		triggers.push_back({trigger.sql_body->Copy(), trigger.for_each});
	});
	return triggers;
}

PhysicalOperator &PhysicalTrigger::WrapIfNeeded(ClientContext &context, PhysicalPlanGenerator &planner,
                                                PhysicalOperator &op, TableCatalogEntry &table, TriggerTiming timing,
                                                TriggerEventType event_type) {
	auto collected = CollectTriggers(context, table, timing, event_type);
	if (collected.empty()) {
		return op;
	}
	auto &trigger = planner.Make<PhysicalTrigger>(std::move(collected), 1);
	trigger.children.push_back(op);
	return trigger;
}

PhysicalTrigger::PhysicalTrigger(PhysicalPlan &physical_plan, vector<TriggerInfo> triggers,
                                 idx_t estimated_cardinality)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::TRIGGER, {LogicalType::BIGINT}, estimated_cardinality),
      triggers(std::move(triggers)) {
}

unique_ptr<GlobalSinkState> PhysicalTrigger::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<TriggerGlobalSinkState>();
}

SinkResultType PhysicalTrigger::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	auto &gstate = input.global_state.Cast<TriggerGlobalSinkState>();
	// The operator (INSERT/UPDATE/DELETE) outputs a single BIGINT row with the affected row count
	// TODO - This will be change when `RETURNING` is supported (for NEW/OLD, REFERENCING NEW TABLE AS)
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
