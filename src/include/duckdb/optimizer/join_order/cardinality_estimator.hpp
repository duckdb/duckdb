//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/cardinality_estimator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/reference_map.hpp"
#include "duckdb/optimizer/relation_statistics/cardinality_value.hpp"
#include "duckdb/optimizer/join_order/join_order_operator.hpp"
#include "duckdb/optimizer/join_order/join_relation_set.hpp"
#include "duckdb/optimizer/join_order/join_order_source_stats.hpp"
#include "duckdb/optimizer/relation_statistics/relation_statistics_helper.hpp"

namespace duckdb {

class FilterInfo;
class JoinPredicateModel;
struct CardinalityEstimatorState;
struct FilterInfoWithTotalDomains;
struct SelectivitySubgraph;
struct LogicalJoinStats;

class CardinalityEstimator {
public:
	CardinalityEstimator(JoinRelationSetManager &set_manager, const JoinPredicateModel &predicate_model);
	~CardinalityEstimator();

public:
	//! Initialize immutable logical statistics before estimating any relation set.
	void Initialize(vector<RelationStats> relations, const vector<unique_ptr<JoinOrderOperator>> &operators = {},
	                const join_column_sources_t &sources = {});
	//! All equivalent plans in a memo group share this estimate.
	CardinalityValue EstimateCardinality(JoinRelationSet &set);
	//! Cost adaptation never changes the memoized raw point.
	CardinalityValue EstimateCardinalityForCost(JoinRelationSet &set);
	//! Project immutable region statistics, independently of the selected physical split.
	optional<RelationStats> ProjectStats(JoinRelationSet &set, const vector<ColumnBinding> &bindings);

private:
	class ColumnProjection;
	class CompositeJoinPairCaps;
	struct SelectivityState;

	optional_ptr<const LogicalJoinStats> FindBoundary(JoinRelationSet &set, optional_ptr<JoinRelationSet> &region);
	optional<RelationColumnStats> ProjectColumn(JoinRelationSet &set, ColumnBinding binding);
	SemiAntiJoinCardinalityEstimate EstimateMembership(const LogicalJoinStats &boundary, JoinRelationSet &left,
	                                                   JoinRelationSet &right, vector<SemiAntiJoinDomain> &domains);
	void InitEquivalentRelations();
	void UpdateTotalDomains(RelationIndex relation, const RelationStats &stats, const join_column_sources_t &sources);
	void RestrictFilterDomains(const vector<RelationStats> &relation_stats);
	bool IsFilterApplicable(const FilterInfo &filter, JoinRelationSet &set) const;
	CardinalityValue GetNumerator(JoinRelationSet &set);
	CardinalityValue GetInnerSelectivity(JoinRelationSet &set);
	CardinalityValue GetJoinSelectivity(JoinRelationSet &set, JoinRelationSet &left, JoinRelationSet &right);
	CardinalityValue GetSelectivity(JoinRelationSet &set);
	void ProcessSelectivityEdge(FilterInfoWithTotalDomains &edge, JoinRelationSet &requested_set,
	                            SelectivityState &state);
	void CreateSelectivitySubgraph(FilterInfoWithTotalDomains &edge, JoinRelationSet &edge_left_set,
	                               JoinRelationSet &edge_right_set, SelectivityState &state);
	void ExtendSelectivitySubgraph(idx_t subgraph_index, FilterInfoWithTotalDomains &edge,
	                               JoinRelationSet &edge_left_set, JoinRelationSet &edge_right_set,
	                               bool can_increment_existing_join, SelectivityState &state);
	void MergeSelectivitySubgraphs(const vector<idx_t> &subgraph_connections, FilterInfoWithTotalDomains &edge,
	                               SelectivityState &state);
	void MergeDisconnectedSelectivitySubgraphs(SelectivityState &state);
	bool SingleColumnFilter(const FilterInfo &filter_info);

	//! Selectivity calculation
	CardinalityValue CalculateInnerJoinSelectivity(CardinalityValue base_selectivity,
	                                               FilterInfoWithTotalDomains &filter);
	CardinalityValue GetLeftJoinSelectivity(const JoinOrderOperator &join_operator);
	CardinalityValue GetJoinPairCap(JoinRelationSet &join_pair);
	//! The number of key combinations of a join pair whose equalities cover a unique key of one of its relations,
	//! or 0 if they cover neither relation's key.
	CardinalityValue GetUniqueJoinPairDomain(JoinRelationSet &join_pair);

	void AddRelationStats(const FilterInfo &filter_info);
	bool EmptyFilter(const FilterInfo &filter_info);

private:
	unique_ptr<CardinalityEstimatorState> state;
	JoinRelationSetManager &set_manager;
	const JoinPredicateModel &predicate_model;
};

} // namespace duckdb
