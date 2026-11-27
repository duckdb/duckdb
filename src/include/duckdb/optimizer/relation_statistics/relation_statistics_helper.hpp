//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/optional.hpp"
#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics.hpp"
#include "duckdb/planner/filter/expression_filter.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class ClientContext;
class Expression;
class LogicalAggregate;
class LogicalColumnDataGet;
class LogicalComparisonJoin;
class LogicalDelimGet;
class LogicalDummyScan;
class LogicalEmptyResult;
class LogicalExpressionGet;
class LogicalFilter;
class LogicalGet;
class LogicalLimit;
class LogicalProjection;
class LogicalWindow;
class RelationStatsExtractor;
class NodeStatistics;

struct SemiAntiJoinDomain {
	CardinalityValue total_domain;
	CardinalityValue rhs_current_domain;
	ColumnBinding preserved_binding;
	ColumnBinding rhs_binding;
};

struct SemiAntiJoinCardinalityEstimate {
	CardinalityValue cardinality;
	bool used_domain_evidence;
};

struct TupleDomainRestrictionEstimate {
	double rows;
	double groups;
};

class RelationStatisticsHelper {
public:
	static constexpr double DEFAULT_SELECTIVITY = 0.2;

public:
	static RelationStats ExtractGetStats(LogicalGet &get, ClientContext &context);
	static RelationStats ExtractGetStats(LogicalGet &get, ClientContext &context, idx_t source_cardinality);
	static RelationStats ExtractGetStats(LogicalGet &get, ClientContext &context, const NodeStatistics &source_stats);
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
	                                                       const RelationStats &child_stats, ClientContext &context);
	//! Reuse the caller snapshot for aggregate input refinements.
	static optional<RelationStats> ExtractAggregationStats(LogicalAggregate &aggregate,
	                                                       const RelationStats &child_stats,
	                                                       RelationStatsExtractor &extractor);
	static optional<RelationStats> ExtractWindowStats(LogicalWindow &window, const RelationStats &child_stats);
	static optional<RelationStats> ExtractFilterStats(LogicalFilter &filter, const RelationStats &child_stats);
	static optional<RelationStats> ExtractFilterStats(LogicalFilter &filter, const RelationStats &child_stats,
	                                                  const vector<unique_ptr<Expression>> &expressions);
	//! Condition an input snapshot without requiring an attached logical operator or changing its layout.
	static RelationStats ConditionFilterStats(const RelationStats &input,
	                                          const vector<unique_ptr<Expression>> &expressions,
	                                          const vector<ColumnBinding> &bindings);
	static optional<RelationStats> ExtractLimitStats(LogicalLimit &limit, const RelationStats &child_stats);
	static RelationStats ExtractEmptyResultStats(LogicalEmptyResult &empty);
	static optional<RelationStats> ProjectOutputStats(const RelationStats &stats, LogicalOperator &op);
	static optional<RelationStats> RebindOutputStats(const RelationStats &stats, LogicalOperator &op);
	static optional<DistinctCount> SelectTotalDomain(const vector<DistinctCount> &domains);
	static void ApplySemiAntiJoinStats(LogicalComparisonJoin &join, const RelationStats &left,
	                                   const RelationStats &right, RelationStats &result);
	static idx_t CardinalityFromDouble(double cardinality);
	static CardinalityValue EstimateDistinctCardinality(const vector<DistinctCount> &distinct_counts,
	                                                    CardinalityValue input_cardinality);
	static CardinalityValue EstimateGroupCardinality(LogicalOperator &input, const RelationStats &input_stats,
	                                                 vector<idx_t> groups, RelationStatsExtractor &extractor);
	static DistinctCount EstimateCurrentDomain(const DistinctCount &current_domain, CardinalityValue input_cardinality,
	                                           CardinalityValue output_cardinality);
	static void ApplyCardinality(RelationStats &stats, CardinalityValue output_cardinality);
	static optional_idx AddCardinalityBounds(optional_idx left, optional_idx right);
	static optional_idx MultiplyCardinalityBounds(optional_idx left, optional_idx right);
	static optional_idx JoinCardinalityBound(JoinType type, optional_idx left, optional_idx right);
	//! Share fallback policy between ordinary statistics and scaled join cardinalities.
	static CardinalityValue EstimateSemiAntiJoinFallbackCardinality(CardinalityValue preserved_cardinality,
	                                                                CardinalityValue rhs_cardinality,
	                                                                JoinType join_type) {
		auto is_anti = join_type == JoinType::ANTI || join_type == JoinType::RIGHT_ANTI;
		if (rhs_cardinality.IsZero()) {
			return is_anti ? preserved_cardinality : CardinalityValue(0);
		}
		return preserved_cardinality * CardinalityValue(DEFAULT_SELECTIVITY);
	}
	static SemiAntiJoinCardinalityEstimate EstimateSemiAntiJoinFallback(CardinalityValue preserved_cardinality,
	                                                                    CardinalityValue rhs_cardinality,
	                                                                    JoinType join_type);
	static SemiAntiJoinCardinalityEstimate
	EstimateSemiAntiJoinCardinality(CardinalityValue preserved_cardinality, CardinalityValue rhs_cardinality,
	                                CardinalityValue rhs_row_retention, JoinType join_type,
	                                const vector<SemiAntiJoinDomain> &domains, bool has_residual);
	static SemiAntiJoinCardinalityEstimate
	EstimateSemiAntiJoinCardinality(LogicalComparisonJoin &join, const RelationStats &left, const RelationStats &right);
	//! Estimate rows and groups for a matched tuple restriction with column-reference conditions.
	static optional<TupleDomainRestrictionEstimate> EstimateTupleDomainRestriction(LogicalComparisonJoin &restriction,
	                                                                               const RelationStats &input,
	                                                                               const RelationStats &keys,
	                                                                               RelationStatsExtractor &extractor);
	static optional_idx EstimateDirectFilterDomain(const Expression &expression, optional<ColumnBinding> target_binding,
	                                               idx_t maximum_domain);

private:
	static unique_ptr<BaseStatistics> GetColumnStatistics(LogicalGet &get, ClientContext &context,
	                                                      const ColumnIndex &column_id);
};

} // namespace duckdb
