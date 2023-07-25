//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/cardinality_estimator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"

namespace duckdb {

struct RelationAttributes {
	string original_name;
	// the relation columns used in join filters
	// Needed when iterating over columns and initializing total domain values.
	unordered_set<idx_t> columns;
	idx_t cardinality;
};

class JoinNode;
struct FilterInfo;
struct SingleJoinRelation;
struct RelationStats;

struct RelationsToTDom {
	//! column binding sets that are equivalent in a join plan.
	//! if you have A.x = B.y and B.y = C.z, then one set is {A.x, B.y, C.z}.
	column_binding_set_t equivalent_relations;
	//!	the estimated total domains of the equivalent relations determined using HLL
	idx_t tdom_hll;
	//! the estimated total domains of each relation without using HLL
	idx_t tdom_no_hll;
	bool has_tdom_hll;
	vector<FilterInfo *> filters;

	vector<string> column_names;

	RelationsToTDom(const column_binding_set_t &column_binding_set)
	    : equivalent_relations(column_binding_set),
	      tdom_hll(0), tdom_no_hll(NumericLimits<idx_t>::Maximum()),
	      has_tdom_hll(false) {};
};

struct NodeOp {
	unique_ptr<JoinNode> node;
	LogicalOperator &op;

	NodeOp(unique_ptr<JoinNode> node, LogicalOperator &op) : node(std::move(node)), op(op) {};
};

struct Subgraph2Denominator {
	unordered_set<idx_t> relations;
	double denom;

	Subgraph2Denominator() : relations(), denom(1) {};
};

class CardinalityHelper {
public:
	CardinalityHelper() {
	}
	CardinalityHelper(idx_t cardinality_before_filters, double filter_string)
	    : cardinality_before_filters(cardinality_before_filters), filter_strength(filter_string) {};

public:
	idx_t cardinality_before_filters;
	double filter_strength;

	vector<string> table_names_joined;
	vector<string> column_names;
};

class CardinalityEstimator {
public:
	explicit CardinalityEstimator() {};

private:
	//! A mapping of relation id -> RelationAttributes
	unordered_map<idx_t, RelationAttributes> relation_attributes;
	//! A mapping of (relation, bound_column) -> (actual table, actual column)
	column_binding_map_t<ColumnBinding> relation_column_to_original_column;

	vector<RelationsToTDom> relations_to_tdoms;
	unordered_map<string, CardinalityHelper> relation_set_2_cardinality;
	JoinRelationSetManager set_manager;
	vector<RelationStats> relation_stats;

public:

	void AddRelationNamesToTdoms(vector<RelationStats> &stats);
	void PrintRelationToTdomInfo();

	//	void AddRelationColumnMapping(LogicalGet &get, idx_t relation_id);

	void InitTotalDomains();
	void InitStats(vector<RelationStats> relation_stats);
	void UpdateTotalDomains(optional_ptr<JoinRelationSet> set, RelationStats &stats);
	void InitEquivalentRelations(const vector<unique_ptr<FilterInfo>> &filter_infos);

	void InitCardinalityEstimatorProps(optional_ptr<JoinRelationSet> set, RelationStats &stats);
	double EstimateCardinalityWithSet(JoinRelationSet &new_set);
	void EstimateBaseTableCardinality(JoinNode &node, LogicalOperator &op);

private:
	bool SingleColumnFilter(FilterInfo &filter_info);
	//! Filter & bindings -> list of indexes into the equivalent_relations array.
	// The column binding set at each index is an equivalence set.
	vector<idx_t> DetermineMatchingEquivalentSets(FilterInfo *filter_info);

	//! Given a filter, add the column bindings to the matching equivalent set at the index
	//! given in matching equivalent sets.
	//! If there are multiple equivalence sets, they are merged.
	void AddToEquivalenceSets(FilterInfo *filter_info, vector<idx_t> matching_equivalent_sets);

	//	optional_ptr<TableFilterSet> GetTableFilters(LogicalOperator &op, idx_t table_index);

	void AddRelationTdom(FilterInfo &filter_info);
	bool EmptyFilter(FilterInfo &filter_info);

	//	idx_t InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter &fil,
	//	                            unique_ptr<BaseStatistics> base_stats);
	//	idx_t InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter &fil,
	//	                           unique_ptr<BaseStatistics> base_stats);
	//	idx_t InspectTableFilters(idx_t cardinality, LogicalOperator &op, TableFilterSet &table_filters, idx_t
	//table_index);
};

} // namespace duckdb
