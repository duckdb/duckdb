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
class RelationManager;

struct GenerateJoinRelation {
	GenerateJoinRelation(JoinRelationSet &set, unique_ptr<LogicalOperator> op_p) : set(set), op(std::move(op_p)) {
	}

	JoinRelationSet &set;
	unique_ptr<LogicalOperator> op;
};

struct FilterInfo {
	FilterInfo(unique_ptr<Expression> filter, JoinRelationSet &set, idx_t filter_index) : filter(std::move(filter)),
	      set(set),
	      filter_index(filter_index) {}


	unique_ptr<Expression> filter;
	JoinRelationSet &set;
	idx_t filter_index;
	optional_ptr<JoinRelationSet> left_set;
	optional_ptr<JoinRelationSet> right_set;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
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

	unique_ptr<LogicalOperator> Reconstruct(unique_ptr<LogicalOperator> plan, JoinNode &node);

	bool HasQueryGraph();

	const vector<unique_ptr<FilterInfo>> GetFilterBindings();

	QueryGraph query_graph;

private:
	vector<optional_ptr<LogicalOperator>> filter_operators;

	// The set of filters extracted from the query graph
//	vector<unique_ptr<Expression>> filters_tmp;
	//! Filter information including the column_bindings that join filters
	//! used by the cardinality estimator to estimate distinct counts
	vector<unique_ptr<FilterInfo>> filters_and_bindings;

	bool has_query_graph;


	void GetColumnBinding(Expression &expression, ColumnBinding &binding);

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
