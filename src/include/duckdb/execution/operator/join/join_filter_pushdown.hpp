//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/join_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/column_binding.hpp"

namespace duckdb {
class DataChunk;
class DynamicTableFilterSet;
struct GlobalUngroupedAggregateState;
struct LocalUngroupedAggregateState;

struct JoinFilterPushdownColumn {
	//! The join condition from which this filter pushdown is generated
	idx_t join_condition;
	//! The probe column index to which this filter should be applied
	ColumnBinding probe_column_index;
};

struct JoinFilterGlobalState {
	~JoinFilterGlobalState();

	//! Global Min/Max aggregates for filter pushdown
	unique_ptr<GlobalUngroupedAggregateState> global_aggregate_state;
};

struct JoinFilterLocalState {
	~JoinFilterLocalState();

	//! Local Min/Max aggregates for filter pushdown
	unique_ptr<LocalUngroupedAggregateState> local_aggregate_state;
};

struct JoinFilterPushdownInfo {
	//! The dynamic table filter set where to push filters into
	shared_ptr<DynamicTableFilterSet> dynamic_filters;
	//! The filters that we should generate
	vector<JoinFilterPushdownColumn> filters;
	//! Min/Max aggregates
	vector<unique_ptr<Expression>> min_max_aggregates;

public:
	unique_ptr<JoinFilterGlobalState> GetGlobalState(ClientContext &context, const PhysicalOperator &op) const;
	unique_ptr<JoinFilterLocalState> GetLocalState(JoinFilterGlobalState &gstate) const;

	void Sink(DataChunk &chunk, JoinFilterLocalState &lstate) const;
	void Combine(JoinFilterGlobalState &gstate, JoinFilterLocalState &lstate) const;
	void PushFilters(JoinFilterGlobalState &gstate, const PhysicalOperator &op) const;
};

} // namespace duckdb
