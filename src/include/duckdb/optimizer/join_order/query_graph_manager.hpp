//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/query_graph_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/reference_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_predicate.hpp"
#include "duckdb/optimizer/join_order/join_relation_set.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/optimizer/join_order/join_order_operator.hpp"

namespace duckdb {

class QueryGraphEdges;
class CardinalityEstimator;

struct GenerateJoinRelation {
public:
	GenerateJoinRelation(optional_ptr<JoinRelationSet> set, unique_ptr<LogicalOperator> op_p, RelationStats stats_p);

public:
	optional_ptr<JoinRelationSet> set;
	unique_ptr<LogicalOperator> op;
	RelationStats stats;
};

enum class PlanEstimateUpdate { KEEP, UPDATE };

//! The QueryGraphManager manages the process of extracting the reorderable and nonreorderable operations
//! from the logical plan and creating the intermediate structures needed by the plan enumerator.
//! When the plan enumerator finishes, the Query Graph Manger can then recreate the logical plan.
class QueryGraphManager {
public:
	explicit QueryGraphManager(ClientContext &context);

public:
	//! Extract the join relations, optimizing non-reoderable relations when encountered
	bool Build(JoinOrderOptimizer &optimizer, LogicalOperator &op);
	//! Prepare an inner-join region without mutating or enumerating its plan.
	bool BuildForEstimation(RelationStatsExtractor &extractor, LogicalOperator &op);
	optional<RelationStats> ProjectStats(CardinalityEstimator &estimator, JoinRelationSet &set,
	                                     const vector<ColumnBinding> &bindings) const;
	//! Project a complete region through its original unary ancestors without reconstructing the join order.
	optional<RelationStats> ProjectRetainedStats(CardinalityEstimator &estimator, JoinRelationSet &set,
	                                             LogicalOperator &plan,
	                                             PlanEstimateUpdate update_estimates = PlanEstimateUpdate::KEEP);
	//! Reconstruct the logical plan using the plan found by the plan enumerator
	GenerateJoinRelation Reconstruct(unique_ptr<LogicalOperator> plan, CardinalityEstimator &estimator);
	//! Plan enumerator may not find a full plan and therefore will need to create cross  products to create edges.
	void CreateQueryGraphCrossProduct(JoinRelationSet &left, JoinRelationSet &right);
	//! Admit original cross-product edges when the current hyperedges cannot complete enumeration.
	bool ActivateRequiredCrossProducts();
	//! Whether the predicate graph still has disconnected components.
	bool RequiresCrossProduct() const;
	bool IsConnectionApplicable(const NeighborInfo &connection, const JoinRelationSet &left,
	                            const JoinRelationSet &right) const;
	bool IsJoinOrderCandidate(optional_ptr<JoinOrderOperator> selected_operator, bool generated_cross_product,
	                          JoinRelationSet &left, JoinRelationSet &right);
	bool CanCreateCrossProduct(const JoinRelationSet &left, const JoinRelationSet &right) const;

	//! Get a reference to the QueryGraphEdges structure that stores edges between nodes and hypernodes.
	const QueryGraphEdges &GetQueryGraphEdges() const;
	const JoinPredicateModel &GetPredicateModel() const;
	const vector<unique_ptr<JoinOrderOperator>> &GetJoinOperators() const;

private:
	void PreparePredicateModel(LogicalOperator &op);
	void GetColumnBinding(const Expression &expression, ColumnBinding &binding);
	void GetEquivalenceBinding(const Expression &expression, ColumnBinding &binding);

	void BindFilterEndpoints();
	void CreateHyperGraphEdges();

	//! Build the normalized predicate model after filter endpoints and stats bindings are populated.
	void BuildPredicateModel();
	void BuildLeftJoinPredicates();
	void BuildInnerCompanionSets();
	void ClearExtractedExpressions();
	vector<reference<FilterInfo>> GetPendingFilters(JoinRelationSet &root) const;
	RelationStats ApplyAncestorStats(const vector<reference<LogicalOperator>> &ancestors, RelationStats stats,
	                                 PlanEstimateUpdate update_estimates);
	idx_t FindGraphComponent(RelationIndex relation);
	idx_t GetGraphComponent(RelationIndex relation) const;
	void ConnectGraphComponents(const JoinRelationSet &left, const JoinRelationSet &right);

	GenerateJoinRelation GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinRelationSet &set,
	                                   CardinalityEstimator &estimator,
	                                   const vector<reference<FilterInfo>> &pending_filters);

public:
	ClientContext &context;
	//! manage relations and the logical operators they represent
	RelationManager relation_manager;
	//! A structure holding all the created JoinRelationSet objects
	JoinRelationSetManager set_manager;
	//! A map to store the optimal join plan found for a specific JoinRelationSet
	optional_ptr<const reference_map_t<JoinRelationSet, unique_ptr<DPJoinNode>>> plans;

private:
	vector<reference<LogicalOperator>> filter_operators;

	//! Filter information including the column_bindings that join filters
	//! used by the cardinality estimator to estimate distinct counts
	vector<unique_ptr<FilterInfo>> filters_and_bindings;
	expression_set_t extracted_filter_expressions;
	vector<unique_ptr<JoinOrderOperator>> join_operators;
	//! Only these operator occurrences can restrict a candidate join.
	vector<reference<const JoinOrderOperator>> candidate_constraints;
	unordered_set<idx_t> operator_costing_predicates;
	unordered_set<idx_t> reconstructed_operators;
	vector<idx_t> inner_companion_roots;
	vector<idx_t> graph_component_roots;
	bool required_cross_products_activated = false;

	QueryGraphEdges query_graph;
	JoinPredicateModel predicate_model;
};

} // namespace duckdb
