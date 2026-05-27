//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/cardinality_estimator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"

#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"

namespace duckdb {

class FilterInfo;
class JoinPredicate;
class JoinPredicateModel;
struct DenominatorState;

struct DenomInfo {
	DenomInfo(JoinRelationSet &numerator_relations, double denominator)
	    : numerator_relations(numerator_relations), denominator(denominator) {
	}

	JoinRelationSet &numerator_relations;
	double denominator;
};

struct RelationsSetToStats {
	//! column binding sets that are equivalent in a join plan.
	//! if you have A.x = B.y and B.y = C.z, then one set is {A.x, B.y, C.z}.
	column_binding_set_t equivalent_relations;
	//!	the estimated total domains of the equivalent relations determined using HLL
	idx_t distinct_count_hll;
	//! the estimated total domains of each relation without using HLL
	idx_t distinct_count_no_hll;
	bool has_distinct_count_hll;
	vector<reference<JoinPredicate>> predicates;
	vector<string> column_names;

	explicit RelationsSetToStats(const column_binding_set_t &column_binding_set)
	    : equivalent_relations(column_binding_set), distinct_count_hll(0),
	      distinct_count_no_hll(NumericLimits<idx_t>::Maximum()), has_distinct_count_hll(false) {};
};

// class to wrap a join Filter along with some statistical information about the joined columns
class FilterInfoWithTotalDomains {
public:
	FilterInfoWithTotalDomains(JoinPredicate &predicate, RelationsSetToStats &relation_set_to_stats)
	    : predicate(predicate), distinct_count_hll(relation_set_to_stats.distinct_count_hll),
	      distinct_count_no_hll(relation_set_to_stats.distinct_count_no_hll),
	      has_distinct_count_hll(relation_set_to_stats.has_distinct_count_hll) {
	}

	double GetDistinctCount() const {
		return static_cast<double>(has_distinct_count_hll ? distinct_count_hll : distinct_count_no_hll);
	}

	//! Extract the comparison type (EQUAL, LESSTHAN, etc.) from a join filter expression.
	ExpressionType GetComparisonType();
	//! Whether this is an INNER equality filter
	bool IsInnerEquality();
	FilterInfo &GetFilter() const;
	JoinPredicate &GetPredicate() const;

	reference<JoinPredicate> predicate;
	//!	the estimated distinct count the joined columns determined using HLL
	idx_t distinct_count_hll;
	//! the estimated total domains of each relation without using HLL
	idx_t distinct_count_no_hll;
	bool has_distinct_count_hll;
};

struct Subgraph2Denominator {
	optional_ptr<JoinRelationSet> relations;
	optional_ptr<JoinRelationSet> numerator_relations;
	double denom;

	Subgraph2Denominator() : relations(nullptr), numerator_relations(nullptr), denom(1) {};
};

struct CompositeJoinPairStats {
	// The row-count cap is only plausible when the candidate key cardinality is within the same order of magnitude
	// as an observed single-column domain. Otherwise broad fact-to-fact joins can look like key lookups.
	static constexpr double MAX_CARDINALITY_TO_DISTINCT_RATIO = 8;

	double first_distinct_count = 0;
	double max_distinct_count = 0;
	bool has_distinct_count = false;

	void RegisterDistinctCount(double distinct_count) {
		if (!has_distinct_count) {
			first_distinct_count = distinct_count;
			has_distinct_count = true;
		}
		if (distinct_count > max_distinct_count) {
			max_distinct_count = distinct_count;
		}
	}

	bool CanApplyCap(double cap) const {
		return has_distinct_count && max_distinct_count > 0 &&
		       cap <= max_distinct_count * MAX_CARDINALITY_TO_DISTINCT_RATIO;
	}
};

class CardinalityHelper {
public:
	CardinalityHelper() {
	}
	explicit CardinalityHelper(double cardinality_before_filters)
	    : cardinality_before_filters(cardinality_before_filters) {};

public:
	// must be a double. Otherwise we can lose significance between different join orders.
	// our cardinality estimator severely underestimates cardinalities for 3+ joins. However,
	// if one join order has an estimate of 0.8, and another has an estimate of 0.6, rounding
	// them means there is no estimated difference, when in reality there could be a very large
	// difference.
	double cardinality_before_filters;

	vector<string> table_names_joined;
	vector<string> column_names;
};

class CardinalityEstimator {
public:
	static constexpr double DEFAULT_SEMI_ANTI_SELECTIVITY = 5;
	explicit CardinalityEstimator(JoinRelationSetManager &set_manager, const JoinPredicateModel &predicate_model)
	    : set_manager(set_manager), predicate_model(predicate_model) {
	}

private:
	vector<RelationsSetToStats> relation_set_stats;
	reference_map_t<JoinRelationSet, CardinalityHelper> relation_set_2_cardinality;
	reference_map_t<JoinRelationSet, reference_map_t<JoinRelationSet, idx_t>> active_equality_class_count_cache;
	JoinRelationSetManager &set_manager;
	const JoinPredicateModel &predicate_model;
	vector<RelationStats> relation_stats;

public:
	void RemoveEmptyTotalDomains();
	void UpdateTotalDomains(optional_ptr<JoinRelationSet> set, RelationStats &stats);
	void InitEquivalentRelations();

	void InitCardinalityEstimatorProps(optional_ptr<JoinRelationSet> set, RelationStats &stats);

	//! cost model needs estimated cardinalities to the fraction since the formula captures
	//! distinct count selectivities and multiplicities. Hence the template
	template <class T>
	T EstimateCardinalityWithSet(JoinRelationSet &new_set);

	//! used for debugging.
	void AddRelationNamesToRelationStats(vector<RelationStats> &stats);
	void PrintRelationStats();

private:
	double GetNumerator(JoinRelationSet &set);
	DenomInfo GetDenominator(JoinRelationSet &set);
	void ProcessDenominatorEdge(FilterInfoWithTotalDomains &edge, JoinRelationSet &requested_set,
	                            DenominatorState &state);
	void CreateDenominatorSubgraph(FilterInfoWithTotalDomains &edge, JoinRelationSet &edge_left_set,
	                               JoinRelationSet &edge_right_set, DenominatorState &state);
	void ExtendDenominatorSubgraph(idx_t subgraph_index, FilterInfoWithTotalDomains &edge,
	                               JoinRelationSet &edge_left_set, JoinRelationSet &edge_right_set,
	                               bool can_increment_existing_join, DenominatorState &state);
	void MergeDenominatorSubgraphs(const vector<idx_t> &subgraph_connections, FilterInfoWithTotalDomains &edge,
	                               DenominatorState &state);
	void MergeDisconnectedDenominatorSubgraphs(DenominatorState &state);
	void AddCrossProductRelations(JoinRelationSet &set, DenominatorState &state);
	DenomInfo CreateDenominatorResult(JoinRelationSet &set, DenominatorState &state);
	//! Applied outside the cardinality cache so stored values stay pre-OR.
	double ApplyOrFilterSelectivities(JoinRelationSet &new_set, double cardinality) const;
	idx_t GetActiveEqualityClassCount(JoinRelationSet &pair, JoinRelationSet &scope);

	vector<optional_ptr<FilterInfo>> or_filters;

	bool SingleColumnFilter(const FilterInfo &filter_info);

	//! Denom calculation
	double CalculateUpdatedDenom(Subgraph2Denominator left, Subgraph2Denominator right,
	                             FilterInfoWithTotalDomains &filter);
	double CalculateInnerJoinDenom(double base_denom, FilterInfoWithTotalDomains &filter);
	double CalculateLeftJoinDenom(Subgraph2Denominator &left, Subgraph2Denominator &right,
	                              FilterInfoWithTotalDomains &filter);
	double CalculateSemiAntiJoinDenom(double base_denom, Subgraph2Denominator &left, Subgraph2Denominator &right,
	                                  FilterInfoWithTotalDomains &filter);
	bool ApplyJoinIncrement(double &target_denom, FilterInfoWithTotalDomains &edge,
	                        reference_map_t<JoinRelationSet, CompositeJoinPairStats> &inner_join_pair_stats,
	                        reference_set_t<JoinRelationSet> &capped_join_pairs, JoinRelationSet &scope,
	                        optional_ptr<JoinRelationSet> join_pair = nullptr);
	bool ApplyJoinPairCap(double &target_denom, JoinRelationSet &join_pair,
	                      reference_map_t<JoinRelationSet, CompositeJoinPairStats> &inner_join_pair_stats,
	                      reference_set_t<JoinRelationSet> &capped_join_pairs);
	bool ApplyCompositeJoinPairCaps(double &target_denom, JoinRelationSet &scope,
	                                reference_map_t<JoinRelationSet, CompositeJoinPairStats> &inner_join_pair_stats,
	                                reference_set_t<JoinRelationSet> &capped_join_pairs);
	double GetJoinPairCap(JoinRelationSet &join_pair);

	JoinRelationSet &UpdateNumeratorRelations(Subgraph2Denominator left, Subgraph2Denominator right,
	                                          FilterInfoWithTotalDomains &filter);

	void AddRelationStats(const FilterInfo &filter_info);
	bool EmptyFilter(const FilterInfo &filter_info);
};

} // namespace duckdb
