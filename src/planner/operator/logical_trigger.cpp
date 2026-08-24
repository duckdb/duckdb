#include "duckdb/planner/operator/logical_trigger.hpp"

namespace duckdb {

LogicalTrigger::LogicalTrigger(string trigger_name_p, TriggerTiming timing_p, TriggerEventType event_type_p,
                               CorrelatedColumns correlated_columns_p)
    : LogicalOperator(LogicalOperatorType::LOGICAL_TRIGGER), trigger_name(std::move(trigger_name_p)), timing(timing_p),
      event_type(event_type_p), correlated_columns(std::move(correlated_columns_p)) {
}

vector<ColumnBinding> LogicalTrigger::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalTrigger::ResolveTypes() {
	types = children[0]->types;
}

} // namespace duckdb
