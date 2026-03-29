#include "duckdb/planner/operator/logical_trigger.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/trigger_catalog_entry.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"

namespace duckdb {

void CollectTriggers(ClientContext &context, TableCatalogEntry &table, TriggerTiming timing,
                     TriggerEventType event_type, vector<unique_ptr<QueryNode>> &trigger_bodies,
                     vector<TriggerForEach> &trigger_for_each) {
	table.ParentSchema().Scan(context, CatalogType::TRIGGER_ENTRY, [&](CatalogEntry &entry) {
		auto &trigger = entry.Cast<TriggerCatalogEntry>();
		if (trigger.timing != timing || trigger.event_type != event_type) {
			return;
		}
		if (trigger.base_table->table_name != table.name) {
			return;
		}
		D_ASSERT(trigger.sql_body);
		trigger_bodies.push_back(trigger.sql_body->Copy());
		trigger_for_each.push_back(trigger.for_each);
	});
}

LogicalTrigger::LogicalTrigger(TableCatalogEntry &table, TriggerTiming timing, TriggerEventType event_type,
                               vector<unique_ptr<QueryNode>> trigger_bodies, vector<TriggerForEach> trigger_for_each)
    : LogicalOperator(LogicalOperatorType::LOGICAL_TRIGGER), table(table), timing(timing), event_type(event_type),
      trigger_bodies(std::move(trigger_bodies)), trigger_for_each(std::move(trigger_for_each)) {
}

LogicalTrigger::LogicalTrigger(ClientContext &context, const unique_ptr<CreateInfo> &table_info, TriggerTiming timing,
                               TriggerEventType event_type)
    : LogicalOperator(LogicalOperatorType::LOGICAL_TRIGGER),
      table(Catalog::GetEntry<TableCatalogEntry>(context, table_info->catalog, table_info->schema,
                                                 table_info->Cast<CreateTableInfo>().table)),
      timing(timing), event_type(event_type) {
}

idx_t LogicalTrigger::EstimateCardinality(ClientContext &context) {
	return 1;
}

vector<ColumnBinding> LogicalTrigger::GetColumnBindings() {
	return {ColumnBinding(TableIndex(0), ProjectionIndex(0))};
}

void LogicalTrigger::ResolveTypes() {
	types.emplace_back(LogicalType::BIGINT);
}

string LogicalTrigger::GetName() const {
	return LogicalOperator::GetName();
}

} // namespace duckdb
