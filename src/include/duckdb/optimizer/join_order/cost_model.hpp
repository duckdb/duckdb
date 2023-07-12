//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/cost_model.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"

namespace duckdb {

class CostModel {
public:
	explicit CostModel(QueryGraphManager query_graph_manager) : query_graph_manager(query_graph_manager) {
	}

private:
	//! query graph storing relation manager information
	QueryGraphManager query_graph_manager;
	//! Cardinality Estimator used to calculate cost
	CardinalityEstimator cardinality_estimator;
	//! map of join relation set to cost
	unordered_map<JoinRelationSet *, idx_t> cost;

public:

	static double ComputeCost(JoinNode &left, JoinNode &right);

private:
	bool SingleColumnFilter(FilterInfo &filter_info);
	//! Filter & bindings -> list of indexes into the equivalent_relations array.
	// The column binding set at each index is an equivalence set.
	vector<idx_t> DetermineMatchingEquivalentSets(FilterInfo *filter_info);

	//! Given a filter, add the column bindings to the matching equivalent set at the index
	//! given in matching equivalent sets.
	//! If there are multiple equivalence sets, they are merged.
	void AddToEquivalenceSets(FilterInfo *filter_info, vector<idx_t> matching_equivalent_sets);

	optional_ptr<TableFilterSet> GetTableFilters(LogicalOperator &op, idx_t table_index);

	void AddRelationTdom(FilterInfo &filter_info);
	bool EmptyFilter(FilterInfo &filter_info);

	idx_t InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter &fil,
	                            unique_ptr<BaseStatistics> base_stats);
	idx_t InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter &fil,
	                           unique_ptr<BaseStatistics> base_stats);
	idx_t InspectTableFilters(idx_t cardinality, LogicalOperator &op, TableFilterSet &table_filters, idx_t table_index);
};

} // namespace duckdb
