//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/join_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {
class DataChunk;
class DynamicTableFilterSet;
class LogicalGet;
class JoinHashTable;
struct GlobalUngroupedAggregateState;
struct LocalUngroupedAggregateState;

struct JoinFilterPushdownColumn {
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

struct JoinFilterPushdownFilter {
	//! The dynamic table filter set where to push filters into
	shared_ptr<DynamicTableFilterSet> dynamic_filters;
	//! The columns for which we should generate filters
	vector<JoinFilterPushdownColumn> columns;
};

struct PushdownFilterTarget {
	PushdownFilterTarget(LogicalGet &get, vector<JoinFilterPushdownColumn> columns_p)
	    : get(get), columns(std::move(columns_p)) {
	}

	LogicalGet &get;
	vector<JoinFilterPushdownColumn> columns;
};

struct JoinFilterPushdownInfo {
	//! The join condition indexes for which we compute the min/max aggregates
	vector<idx_t> join_condition;
	//! The probes to push the filter into
	vector<JoinFilterPushdownFilter> probe_info;
	//! Min/Max aggregates
	vector<unique_ptr<Expression>> min_max_aggregates;

public:
	unique_ptr<JoinFilterGlobalState> GetGlobalState(ClientContext &context, const PhysicalOperator &op) const;
	unique_ptr<JoinFilterLocalState> GetLocalState(JoinFilterGlobalState &gstate) const;

	void Sink(DataChunk &chunk, JoinFilterLocalState &lstate) const;
	void Combine(JoinFilterGlobalState &gstate, JoinFilterLocalState &lstate) const;
	unique_ptr<DataChunk> Finalize(ClientContext &context, JoinHashTable &ht, JoinFilterGlobalState &gstate,
	                               const PhysicalOperator &op) const;

private:
	void PushInFilter(const JoinFilterPushdownFilter &info, JoinHashTable &ht, const PhysicalOperator &op,
	                  idx_t filter_idx, idx_t filter_col_idx) const;
};

} // namespace duckdb
