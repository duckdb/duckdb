//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/trigger_executor.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/trigger_type.hpp"
#include "duckdb/parser/query_node.hpp"

namespace duckdb {
class ClientContext;
class TableCatalogEntry;

void CollectTriggers(ClientContext &context, TableCatalogEntry &table, TriggerTiming timing,
                     TriggerEventType event_type, vector<unique_ptr<QueryNode>> &trigger_bodies,
                     vector<TriggerForEach> &trigger_for_each);

//! LogicalTrigger represents trigger firing for a statement
class LogicalTrigger : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_TRIGGER;

public:
	LogicalTrigger(TableCatalogEntry &table, TriggerTiming timing, TriggerEventType event_type,
	               vector<unique_ptr<QueryNode>> trigger_bodies, vector<TriggerForEach> trigger_for_each);

	TableCatalogEntry &table;
	TriggerTiming timing;
	TriggerEventType event_type;
	//! Trigger bodies - parallel to trigger_for_each
	vector<unique_ptr<QueryNode>> trigger_bodies;
	vector<TriggerForEach> trigger_for_each;

public:
	void Serialize(Serializer &serializer) const override;
	static unique_ptr<LogicalOperator> Deserialize(Deserializer &deserializer);

	idx_t EstimateCardinality(ClientContext &context) override;
	string GetName() const override;

protected:
	vector<ColumnBinding> GetColumnBindings() override;
	void ResolveTypes() override;

private:
	LogicalTrigger(ClientContext &context, const unique_ptr<CreateInfo> &table_info, TriggerTiming timing,
	               TriggerEventType event_type);
};

} // namespace duckdb
