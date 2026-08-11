//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/aggregate_fn_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once
#include "duckdb/optimizer/type_pushdown.hpp"

namespace duckdb {
class LogicalAggregate;

unique_ptr<LogicalOperator> TryPushdownAggregateFunctions(ClientContext &context, unique_ptr<LogicalOperator> plan);

unique_ptr<LogicalOperator> RewriteAggregates(ClientContext &context, unique_ptr<LogicalOperator> op,
                                              Analyses &analyses, const Projections &projections);

unique_ptr<LogicalOperator> TryReplaceAggregate(ClientContext &context, unique_ptr<LogicalOperator> op,
                                                Analyses &analyses, const Projections &projections);

// return GET for UNGROUPED_AGGREGATE -> [GET] or for UNGROUPED_AGGREGATE ->
// PROJECTION -> [GET], nullptr if not found.
LogicalGet *GetChildGet(const LogicalAggregate &agg);

// Push UNGROUPED_AGGREGATE's of form agg(T) and count_star() into GET.
class AggregateFnPushdown {
public:
	explicit inline AggregateFnPushdown(ClientContext &context) : context(context) {
	}
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	ClientContext &context;
};
} // namespace duckdb
