//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;
class LogicalAggregate;
class LogicalColumnDataGet;
class LogicalDelimGet;
class LogicalDistinct;
class LogicalDummyScan;
class LogicalEmptyResult;
class LogicalExpressionGet;
class LogicalGet;
class LogicalProjection;
class LogicalWindow;

class RelationStatisticsHelper {
public:
	static constexpr double DEFAULT_SELECTIVITY = 0.2;

public:
	static idx_t InspectTableFilter(idx_t cardinality, const TableFilter &filter, BaseStatistics &base_stats);
	static RelationStats ExtractGetStats(LogicalGet &get, ClientContext &context);
	static RelationStats ExtractDelimGetStats(LogicalDelimGet &delim_get, ClientContext &context);
	static RelationStats ExtractDummyScanStats(LogicalDummyScan &dummy_scan, ClientContext &context);
	static RelationStats ExtractExpressionGetStats(LogicalExpressionGet &expression_get, ClientContext &context);
	static RelationStats ExtractColumnDataGetStats(LogicalColumnDataGet &column_data_get, ClientContext &context);
	static RelationStats ExtractExplainStats(LogicalOperator &op);
	static optional<RelationStats> ExtractOperatorStats(LogicalOperator &op, ClientContext &context,
	                                                    const vector<reference<const RelationStats>> &child_stats);
	static optional<RelationStats> ExtractProjectionStats(LogicalProjection &projection,
	                                                      const RelationStats &child_stats);
	static optional<RelationStats> ExtractAggregationStats(LogicalAggregate &aggregate,
	                                                       const RelationStats &child_stats);
	static optional<RelationStats> ExtractWindowStats(LogicalWindow &window, const RelationStats &child_stats);
	static optional<RelationStats> ExtractDistinctStats(LogicalDistinct &distinct, const RelationStats &child_stats);
	static RelationStats ExtractEmptyResultStats(LogicalEmptyResult &empty);
	static optional<RelationStats> ProjectOutputStats(const RelationStats &stats, LogicalOperator &op);
	static optional<RelationStats> RebindOutputStats(const RelationStats &stats, LogicalOperator &op);
	static idx_t EstimateDistinctCardinality(const vector<DistinctCount> &distinct_counts, idx_t input_cardinality);

private:
	static unique_ptr<BaseStatistics> GetColumnStatistics(LogicalGet &get, ClientContext &context,
	                                                      const ColumnIndex &column_id);
	static DistinctCount GetDistinctCount(LogicalGet &get, ClientContext &context, const ColumnIndex &column_id,
	                                      idx_t base_table_cardinality);
};

} // namespace duckdb
