//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/operator/join/join_filter_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/projection_index.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/expression.hpp"
#include "duckdb/planner/table_filter.hpp"

namespace duckdb {
class DataChunk;
class DynamicTableFilterSet;
class LogicalGet;
class JoinHashTable;
class PhysicalComparisonJoin;
struct GlobalUngroupedAggregateState;
struct LocalUngroupedAggregateState;

enum class JoinFilterPushdownMode : uint8_t {
	//! The pushed expression can be reconstructed on top of the raw scan value for BF/PRF runtime filters
	RECONSTRUCT_EXPRESSION,
	//! Only storage-domain filters are safe; BF/PRF reconstruction on raw scan values is not
	STORAGE_ONLY
};

struct JoinFilterPushdownColumn {
	//! The probe column index to which this filter should be applied
	ColumnBinding probe_column_index;
	//! The type of the value in storage (LogicalGet)
	LogicalType storage_type;
	//! Whether runtime filters can reconstruct the pushed expression, or whether only storage-domain filters are safe
	JoinFilterPushdownMode mode = JoinFilterPushdownMode::RECONSTRUCT_EXPRESSION;
	//! The original type of the pushed probe expression before rewriting to the LogicalGet storage column. Only used
	//! when the mode allows reconstruction of the probe expression for BF/PRF runtime filters.
	LogicalType runtime_filter_type;
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

struct JoinFilterPushdownUtil {
	static bool PushdownJoinFilterExpression(const Expression &expr, JoinFilterPushdownColumn &filter);
	static bool JoinTypeIsSupported(JoinType join_type);
};

struct JoinFilterPushdownInfo {
	//! The join condition indexes for which we compute the min/max aggregates
	vector<idx_t> join_condition;
	//! The probes to push the filter into
	vector<JoinFilterPushdownFilter> probe_info;
	//! Min/Max aggregates
	vector<unique_ptr<Expression>> min_max_aggregates;
	//! Whether the build side has a filter -> we might be able to push down a bloom filter into the probe side
	bool build_side_has_filter;

public:
	unique_ptr<JoinFilterGlobalState> GetGlobalState(ClientContext &context, const PhysicalOperator &op) const;
	unique_ptr<JoinFilterLocalState> GetLocalState(JoinFilterGlobalState &gstate) const;

	void Sink(DataChunk &chunk, JoinFilterLocalState &lstate) const;
	void Combine(JoinFilterGlobalState &gstate, JoinFilterLocalState &lstate) const;
	unique_ptr<DataChunk> Finalize(ClientContext &context, JoinFilterGlobalState &gstate,
	                               const PhysicalComparisonJoin &op, optional_ptr<JoinHashTable> ht = nullptr) const;

	unique_ptr<DataChunk> FinalizeMinMax(JoinFilterGlobalState &gstate) const;
	unique_ptr<DataChunk> FinalizeFilters(ClientContext &context, const PhysicalComparisonJoin &op,
	                                      unique_ptr<DataChunk> final_min_max, optional_ptr<JoinHashTable> ht = nullptr,
	                                      bool allow_bloom_filters = true,
	                                      bool allow_prefix_range_filters = true) const;

private:
	bool PushInFilter(const JoinFilterPushdownFilter &info, JoinHashTable &ht, const PhysicalOperator &op,
	                  idx_t filter_idx, ProjectionIndex filter_col_idx) const;

	void PushBloomFilter(ClientContext &context, const PhysicalOperator &op, JoinHashTable &ht,
	                     const JoinFilterPushdownFilter &info, idx_t filter_idx, ProjectionIndex filter_col_idx) const;
	bool TryRegisterPrefixRangeFilter(const JoinFilterPushdownFilter &info, ClientContext &context, JoinHashTable &ht,
	                                  const PhysicalOperator &op, idx_t filter_idx, ProjectionIndex filter_col_idx,
	                                  const Value &min_val, const Value &max_val, idx_t max_bits) const;

	bool CanUseInFilter(const ClientContext &context, optional_ptr<JoinHashTable> ht, const ExpressionType &cmp) const;
	bool CanUseBloomFilter(const ClientContext &context, const PhysicalComparisonJoin &op, const ExpressionType &cmp,
	                       optional_ptr<JoinHashTable> ht = nullptr) const;
	bool CanUsePrefixRangeFilter(const ClientContext &context, const PhysicalComparisonJoin &op,
	                             optional_ptr<JoinHashTable> ht, const ExpressionType &cmp) const;
};

} // namespace duckdb
