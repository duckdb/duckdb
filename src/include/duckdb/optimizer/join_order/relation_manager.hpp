//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/join_order/relation_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/relation_statistics_helper.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"

namespace duckdb {

class JoinOrderOptimizer;
class FilterInfo;

//! Represents a single relation and any metadata accompanying that relation
struct SingleJoinRelation {
	LogicalOperator &op;
	optional_ptr<LogicalOperator> parent;
	RelationStats stats;

	SingleJoinRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent) : op(op), parent(parent) {
	}
	SingleJoinRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent, RelationStats stats)
	    : op(op), parent(parent), stats(std::move(stats)) {
	}
};

class RelationManager {
public:
	explicit RelationManager(ClientContext &context) : context(context) {
	}

	idx_t NumRelations();

	bool ExtractJoinRelations(JoinOrderOptimizer &optimizer, LogicalOperator &input_op,
	                          vector<reference<LogicalOperator>> &filter_operators,
	                          optional_ptr<LogicalOperator> parent = nullptr);

	//! for each join filter in the logical plan op, extract the relations that are referred to on
	//! both sides of the join filter, along with the tables & indexes.
	vector<unique_ptr<FilterInfo>> ExtractEdges(LogicalOperator &op,
	                                            vector<reference<LogicalOperator>> &filter_operators,
	                                            JoinRelationSetManager &set_manager);

	//! Extract the set of relations referred to inside an expression
	bool ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings);
	void AddRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent, const RelationStats &stats);

	void AddAggregateOrWindowRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent,
	                                  const RelationStats &stats, LogicalOperatorType op_type);
	vector<unique_ptr<SingleJoinRelation>> GetRelations();

	const vector<RelationStats> GetRelationStats();
	//! A mapping of base table index -> index into relations array (relation number)
	unordered_map<idx_t, idx_t> relation_mapping;

	bool CrossProductWithRelationAllowed(idx_t relation_id);

	void PrintRelationStats();

private:
	ClientContext &context;
	//! Set of all relations considered in the join optimizer
	vector<unique_ptr<SingleJoinRelation>> relations;
	unordered_set<idx_t> no_cross_product_relations;
};

} // namespace duckdb
