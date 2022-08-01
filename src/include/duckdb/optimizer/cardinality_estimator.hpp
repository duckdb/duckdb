//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/cardinality_estimator.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/optimizer/join_node.hpp"
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
};

struct NodeOp {
	unique_ptr<JoinNode> node;
	LogicalOperator *op;

	NodeOp(unique_ptr<JoinNode> node, LogicalOperator *op) : node(move(node)), op(op) {};
};

class CardinalityEstimator {
public:
	explicit CardinalityEstimator(ClientContext &context) : context(context) {
	}

	//! When calculating the cost of a join. Multiple filters may be present.
	//! These values keep track of the lowest cost join
	double lowest_card;

private:
	ClientContext &context;

	//! A mapping of relation id -> RelationAttributes
	unordered_map<idx_t, RelationAttributes> relation_attributes;
	//! A mapping of (relation, bound_column) -> (actual table, actual column)
	column_binding_map_t<ColumnBinding> relation_column_to_original_column;
	//! vector of column binding sets that are equivalent in a join plan.
	//! if you have A.x = B.y and B.y = C.z, then one set is {A.x, B.y, C.z}.
	vector<column_binding_set_t> equivalent_relations;
	//! vector of the same length as equivalent_relations with the total domains of each relation
	//! These total domains are determined using hll
	vector<idx_t> equivalent_relations_tdom_no_hll;
	//! vector of the same length as equivalent_relations with the total domains of each relation
	//! These total domains are determined without using
	vector<idx_t> equivalent_relations_tdom_hll;

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
	double EstimateCardinality(double left_card, double right_card, ColumnBinding left_binding,
	                           ColumnBinding right_binding);
	void EstimateBaseTableCardinality(JoinNode *node, LogicalOperator *op);
	double EstimateCrossProduct(const JoinNode *left, const JoinNode *right);
	void ResetCard();
	static double ComputeCost(JoinNode *left, JoinNode *right, double expected_cardinality);
	void UpdateLowestcard(double old_card);

private:
	bool SingleColumnFilter(FilterInfo *filter_info);
	//! Filter & bindings -> list of indexes into the equivalent_relations array.
	// The column binding set at each index is an equivalence set.
	vector<idx_t> DetermineMatchingEquivalentSets(FilterInfo *filter_info);

	//! Given a filter, add the column bindings to the matching equivalent set at the index
	//! given in matching equivalent sets.
	//! If there are multiple equivalence sets, they are merged.
	void AddToEquivalenceSets(FilterInfo *filter_info, vector<idx_t> matching_equivalent_sets);

	idx_t GetTDom(ColumnBinding binding);
	TableFilterSet *GetTableFilters(LogicalOperator *op);

	idx_t InspectConjunctionAND(idx_t cardinality, idx_t column_index, ConjunctionAndFilter *fil,
	                            TableCatalogEntry *catalog_table);
	idx_t InspectConjunctionOR(idx_t cardinality, idx_t column_index, ConjunctionOrFilter *fil,
	                           TableCatalogEntry *catalog_table);
	idx_t InspectTableFilters(idx_t cardinality, LogicalOperator *op, TableFilterSet *table_filters);
};

} // namespace duckdb
