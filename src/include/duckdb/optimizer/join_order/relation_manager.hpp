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

struct DistinctCount {
	idx_t distinct_count;
	bool from_hll;
};

struct RelationStats {

	vector<DistinctCount> column_distinct_count;
	idx_t cardinality;
	double filter_strength;
	bool stats_initialized = false;

	vector<string> column_names;
	string table_name;

};

//! Represents a single relation and any metadata accompanying that relation
struct SingleJoinRelation {
	LogicalOperator &op;
	optional_ptr<LogicalOperator> parent;
	RelationStats stats;

	SingleJoinRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent) : op(op), parent(parent) {
	}
	SingleJoinRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent, RelationStats stats)
	    : op(op), parent(parent), stats(stats) {
	}
};

class RelationManager {
public:
	explicit RelationManager(ClientContext &context) : context(context) {
	}

	idx_t NumRelations();

	bool ExtractJoinRelations(LogicalOperator &input_op,
	                          vector<reference<LogicalOperator>> &filter_operators,
	                          optional_ptr<LogicalOperator> parent = nullptr);

	vector<unique_ptr<FilterInfo>> ExtractEdges(LogicalOperator &op,
	                  vector<reference<LogicalOperator>> &filter_operators,
	                  JoinRelationSetManager &set_manager);

	bool ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings);
	void AddRelation(LogicalOperator &op, optional_ptr<LogicalOperator> parent, RelationStats stats);
	vector<unique_ptr<SingleJoinRelation>> GetRelations();

	const vector<RelationStats> GetRelationStats();
	//! A mapping of base table index -> index into relations array (relation number)
	unordered_map<idx_t, idx_t> relation_mapping;

	void PrintRelationStats();

private:
	ClientContext &context;
	//! Set of all relations considered in the join optimizer
	vector<unique_ptr<SingleJoinRelation>> relations;
};

} // namespace duckdb
