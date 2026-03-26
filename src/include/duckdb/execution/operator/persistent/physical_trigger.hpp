//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/persistent/physical_trigger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/trigger_executor.hpp"

namespace duckdb {
class ClientContext;
class PhysicalPlanGenerator;
class TableCatalogEntry;

//! PhysicalTrigger fires triggers after a statement completes
class PhysicalTrigger : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::TRIGGER;

public:
	PhysicalTrigger(PhysicalPlan &physical_plan, vector<TriggerInfo> triggers, idx_t estimated_cardinality);

	//! Trigger bodies collected at physical-plan generation time
	vector<TriggerInfo> triggers;

public:
	//! Wraps an existing physical operator with a PhysicalTrigger if matching triggers exist.
	//! Should be running in physical-plan generation time.
	static PhysicalOperator &WrapIfNeeded(ClientContext &context, PhysicalPlanGenerator &planner,
	                                      PhysicalOperator &op, TableCatalogEntry &table, TriggerTiming timing,
	                                      TriggerEventType event_type);

	// Sink interface - receives the row count emitted by the operator
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;
	bool IsSink() const override {
		return true;
	}

	// Source interface - re-emits the row count
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
	                                 OperatorSourceInput &input) const override;
	bool IsSource() const override {
		return true;
	}
};

} // namespace duckdb
