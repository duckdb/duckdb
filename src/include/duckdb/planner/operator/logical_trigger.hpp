//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_trigger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/enums/trigger_type.hpp"

namespace duckdb {

//! LogicalTrigger represents a FOR EACH ROW trigger attached to a DML statement.
//!
//! child[0] = affected rows source (CTE scan of the fired DML's returning output)
//! child[1] = trigger body (correlated subplan NEW.col refs are BoundColumnRef at depth=1)
//!
//! This node is transient: a pre-decorrelation rewrite pass converts it into a LogicalDependentJoin
//!  before FlattenDependentJoins::DecorrelateIndependent runs.
class LogicalTrigger : public LogicalOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_TRIGGER;

public:
	LogicalTrigger(string trigger_name, TriggerTiming timing, TriggerEventType event_type,
	               CorrelatedColumns correlated_columns);

	string trigger_name;
	TriggerTiming timing;
	TriggerEventType event_type;
	//! The NEW.col references from child[1] that correlate with child[0]
	CorrelatedColumns correlated_columns;

protected:
	vector<ColumnBinding> GetColumnBindings() override;
	void ResolveTypes() override;
};

} // namespace duckdb
