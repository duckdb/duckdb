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
#include "duckdb/optimizer/join_order/join_relation.hpp"
#include "duckdb/optimizer/join_order/join_node.hpp"
#include "duckdb/optimizer/join_order/relation_manager.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/planner/column_binding.hpp"

#include <functional>

namespace duckdb {

class Expression;
class LogicalOperator;
class JoinNode;
class RelationManager;

struct GenerateJoinRelation {
	GenerateJoinRelation(JoinRelationSet &set, unique_ptr<LogicalOperator> op_p) : set(set), op(std::move(op_p)) {
	}

	JoinRelationSet &set;
	unique_ptr<LogicalOperator> op;
};

//! The QueryGraph contains edges between relations and allows edges to be created/queried
class QueryGraphManager {
public:
	QueryGraphManager(ClientContext &context) : context(context), has_query_graph(false) {
	}

	//! manage relations and the logical operators they represent
	RelationManager relation_manager;

	//! A structure holding all the created JoinRelationSet objects
	JoinRelationSetManager set_manager;

	//! Extract the join relations, optimizing non-reoderable relations when encountered
	void Build(LogicalOperator *op);

	unique_ptr<LogicalOperator> Reconstruct(JoinNode *root);

	bool HasQueryGraph();

private:
	vector<optional_ptr<LogicalOperator>> filter_operators;

	// The set of filters extracted from the query graph
	vector<unique_ptr<Expression>> filters;

	bool has_query_graph;

	bool ExtractJoinRelations(LogicalOperator &input_op, vector<reference<LogicalOperator>> &filter_operators,
	                          optional_ptr<LogicalOperator> parent);
	bool ExtractEdges(LogicalOperator &op, vector<reference<LogicalOperator>> &filter_operators);
	bool ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings);

	GenerateJoinRelation GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations,
	              JoinNode &node);

	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node);

	ClientContext &context;
};

}
