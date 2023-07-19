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
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/cardinality_estimator.hpp"
#include "duckdb/optimizer/join_order/query_graph.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/parser/expression_map.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/logical_operator_visitor.hpp"


namespace duckdb {

//! Represents a single relation and any metadata accompanying that relation
struct SingleJoinRelation {
	LogicalOperator &op;
	optional_ptr<LogicalOperator> parent;

	SingleJoinRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent) : op(op), parent(parent) {
	}
};

class RelationManager {
public:
	explicit RelationManager(){}

	idx_t NumRelations();
	SingleJoinRelation GetRelation(idx_t relation_id);
	void AddRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent);

private:

	//! Set of all relations considered in the join optimizer
	vector<SingleJoinRelation> relations;
	//! A mapping of base table index -> index into relations array (relation number)
	unordered_map<idx_t, idx_t> relation_mapping;


};

} // namespace duckdb
