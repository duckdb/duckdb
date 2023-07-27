//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/statistics_extractor.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

class CardinalityEstimator;

struct DistinctCount {
	idx_t distinct_count;
	bool from_hll;
};

struct ExpressionBinding {
	bool found_expression = false;
	ColumnBinding child_binding;
	bool expression_is_constant = false;
};

struct RelationStats {

	vector<DistinctCount> column_distinct_count;
	idx_t cardinality;
	double filter_strength;
	bool stats_initialized = false;

	vector<string> column_names;
	string table_name;

};

class RelationStatisticsHelper {
public:
	static constexpr double DEFAULT_SELECTIVITY = 0.2;
public:

	static idx_t InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter &filter,
	                                  unique_ptr<BaseStatistics> base_stats);
	static idx_t InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter &filter,
	                                  unique_ptr<BaseStatistics> base_stats);
	//!
	static RelationStats ExtractGetStats(LogicalGet &get, ClientContext &context);
	static RelationStats ExtractDelimGetStats(LogicalDelimGet &delim_get, ClientContext &context);
	//! Create the statistics for a projection using the statistics of the operator that sits underneath the
	//! projection. Then also create statistics for any extra columns the projection creates.
	static RelationStats ExtractDummyScanStats(LogicalDummyScan &dummy_scan, ClientContext &context);
	static RelationStats ExtractExpressionGetStats(LogicalExpressionGet &expression_get, ClientContext &context);
	//! All relation extractors for blocking relations
	static RelationStats ExtractProjectionStats(LogicalProjection &proj, RelationStats &child_stats);
	static RelationStats ExtractAggregationStats(LogicalAggregate &aggr, RelationStats &child_stats);
	static RelationStats ExtractWindowStats(LogicalWindow &window,  RelationStats &child_stats);

	static RelationStats CombineStatsOfNonReoderableOperator(LogicalOperator &op, vector<RelationStats> child_stats);

private:
};

} // namespace duckdb
