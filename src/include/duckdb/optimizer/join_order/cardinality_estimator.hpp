//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/cardinality_estimator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/column_binding_map.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"

namespace duckdb {

struct RelationAttributes {
	string original_name;
	// the relation columns used in join filters
	// Needed when iterating over columns and initializing total domain values.
	unordered_set<idx_t> columns;
	double cardinality;
};

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

	RelationsToTDom(column_binding_set_t column_binding_set)
	    : equivalent_relations(column_binding_set), tdom_hll(0), tdom_no_hll(NumericLimits<idx_t>::Maximum()),
	      has_tdom_hll(false) {};
};

struct NodeOp {
	unique_ptr<JoinNode> node;
	LogicalOperator *op;

	NodeOp(unique_ptr<JoinNode> node, LogicalOperator *op) : node(move(node)), op(op) {};
};

struct Subgraph2Denominator {
	unordered_set<idx_t> relations;
	double denom;

	Subgraph2Denominator() : relations(), denom(1) {};
};

class CardinalityEstimator {
public:
	explicit CardinalityEstimator(ClientContext &context) : context(context) {
	}

private:
	ClientContext &context;

	//! A mapping of relation id -> RelationAttributes
	unordered_map<idx_t, RelationAttributes> relation_attributes;
	//! A mapping of (relation, bound_column) -> (actual table, actual column)
	column_binding_map_t<ColumnBinding> relation_column_to_original_column;

	vector<RelationsToTDom> relations_to_tdoms;

	static constexpr double DEFAULT_SELECTIVITY = 0.2;

public:
	static void VerifySymmetry(JoinNode *result, JoinNode *entry);

	void AssertEquivalentRelationSize();

	//! given a binding of (relation, column) used for DP, and a (table, column) in that catalog
	//! Add the key value entry into the relation_column_to_original_column
	void AddRelationToColumnMapping(ColumnBinding key, ColumnBinding value);
	//! Add a column to the relation_to_columns map.
	void AddColumnToRelationMap(idx_t table_index, idx_t column_index);
	//! Dump all bindings in relation_column_to_original_column into the child_binding_map
	// If you have a non-reorderable join, this function is used to keep track of bindings
	// in the child join plan.
	void CopyRelationMap(column_binding_map_t<ColumnBinding> &child_binding_map);
	void MergeBindings(idx_t, idx_t relation_id, vector<column_binding_map_t<ColumnBinding>> &child_binding_maps);
	void AddRelationColumnMapping(LogicalGet *get, idx_t relation_id);

	void InitTotalDomains();
	void UpdateTotalDomains(JoinNode *node, LogicalOperator *op);
	void InitEquivalentRelations(vector<unique_ptr<FilterInfo>> *filter_infos);

	void InitCardinalityEstimatorProps(vector<struct NodeOp> *node_ops, vector<unique_ptr<FilterInfo>> *filter_infos);
	double EstimateCardinalityWithSet(JoinRelationSet *new_set);
	void EstimateBaseTableCardinality(JoinNode *node, LogicalOperator *op);
	double EstimateCrossProduct(const JoinNode *left, const JoinNode *right);
	static double ComputeCost(JoinNode *left, JoinNode *right, double expected_cardinality);

private:
	bool SingleColumnFilter(FilterInfo *filter_info);
	//! Filter & bindings -> list of indexes into the equivalent_relations array.
	// The column binding set at each index is an equivalence set.
	vector<idx_t> DetermineMatchingEquivalentSets(FilterInfo *filter_info);

	//! Given a filter, add the column bindings to the matching equivalent set at the index
	//! given in matching equivalent sets.
	//! If there are multiple equivalence sets, they are merged.
	void AddToEquivalenceSets(FilterInfo *filter_info, vector<idx_t> matching_equivalent_sets);

	TableFilterSet *GetTableFilters(LogicalOperator *op);

	void AddRelationTdom(FilterInfo *filter_info);
	bool EmptyFilter(FilterInfo *filter_info);

	idx_t InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter *fil,
	                            unique_ptr<BaseStatistics> base_stats);
	idx_t InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter *fil,
	                           unique_ptr<BaseStatistics> base_stats);
	idx_t InspectTableFilters(idx_t cardinality, LogicalOperator *op, TableFilterSet *table_filters);
};

} // namespace duckdb
