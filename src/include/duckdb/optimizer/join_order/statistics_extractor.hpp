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

namespace duckdb {

class StatisticsExtractor {
public:
	static constexpr double DEFAULT_SELECTIVITY = 0.2;
public:

	static idx_t InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter &filter,
	                                  unique_ptr<BaseStatistics> base_stats);
	static idx_t InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter &filter,
	                                  unique_ptr<BaseStatistics> base_stats);
	//!
	static RelationStats ExtractOperatorStats(LogicalGet &get, ClientContext &context);

	//! Cardinality Estimator used to calculate cost
	CardinalityEstimator cardinality_estimator;

private:
};

} // namespace duckdb
