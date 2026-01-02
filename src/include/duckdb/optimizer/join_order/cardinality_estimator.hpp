//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/cardinality_estimator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"

#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"

namespace duckdb {

class FilterInfo;

struct DenomInfo {
	DenomInfo(JoinRelationSet &numerator_relations, double filter_strength, double denominator)
	    : numerator_relations(numerator_relations), filter_strength(filter_strength), denominator(denominator) {
	}

	JoinRelationSet &numerator_relations;
	double filter_strength;
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
	vector<optional_ptr<FilterInfo>> filters;
	vector<string> column_names;

	explicit RelationsSetToStats(const column_binding_set_t &column_binding_set)
	    : equivalent_relations(column_binding_set), distinct_count_hll(0),
	      distinct_count_no_hll(NumericLimits<idx_t>::Maximum()), has_distinct_count_hll(false) {};
};

// class to wrap a join Filter along with some statistical information about the joined columns
class FilterInfoWithTotalDomains {
public:
	FilterInfoWithTotalDomains(optional_ptr<FilterInfo> filter_info, RelationsSetToStats &relation_set_to_stats)
	    : filter_info(filter_info), distinct_count_hll(relation_set_to_stats.distinct_count_hll),
	      distinct_count_no_hll(relation_set_to_stats.distinct_count_no_hll),
	      has_distinct_count_hll(relation_set_to_stats.has_distinct_count_hll) {
	}

	optional_ptr<FilterInfo> filter_info;
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
	explicit CardinalityEstimator() {};

private:
	vector<RelationsSetToStats> relation_set_stats;
	unordered_map<string, CardinalityHelper> relation_set_2_cardinality;
	JoinRelationSetManager set_manager;
	vector<RelationStats> relation_stats;

public:
	void RemoveEmptyTotalDomains();
	void UpdateTotalDomains(optional_ptr<JoinRelationSet> set, RelationStats &stats);
	void InitEquivalentRelations(const vector<unique_ptr<FilterInfo>> &filter_infos);

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

	bool SingleColumnFilter(FilterInfo &filter_info);
	vector<idx_t> DetermineMatchingEquivalentSets(optional_ptr<FilterInfo> filter_info);
	//! Given a filter, add the column bindings to the matching equivalent set at the index
	//! given in matching equivalent sets.
	//! If there are multiple equivalence sets, they are merged.
	void AddToEquivalenceSets(optional_ptr<FilterInfo> filter_info, vector<idx_t> matching_equivalent_sets);

	double CalculateUpdatedDenom(Subgraph2Denominator left, Subgraph2Denominator right,
	                             FilterInfoWithTotalDomains &filter);
	JoinRelationSet &UpdateNumeratorRelations(Subgraph2Denominator left, Subgraph2Denominator right,
	                                          FilterInfoWithTotalDomains &filter);

	void AddRelationStats(FilterInfo &filter_info);
	bool EmptyFilter(FilterInfo &filter_info);
};

} // namespace duckdb
