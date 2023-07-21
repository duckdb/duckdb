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
	GenerateJoinRelation(optional_ptr<JoinRelationSet> set, unique_ptr<LogicalOperator> op_p)
	    : set(set), op(std::move(op_p)) {
	}

	optional_ptr<JoinRelationSet> set;
	unique_ptr<LogicalOperator> op;
};

struct FilterInfo {
	FilterInfo(unique_ptr<Expression> filter, optional_ptr<JoinRelationSet> set, idx_t filter_index)
	    : filter(std::move(filter)), set(set), filter_index(filter_index) {
	}

	unique_ptr<Expression> filter;
	optional_ptr<JoinRelationSet> set;
	idx_t filter_index;
	optional_ptr<JoinRelationSet> left_set;
	optional_ptr<JoinRelationSet> right_set;
	ColumnBinding left_binding;
	ColumnBinding right_binding;
};

//! The QueryGraph contains edges between relations and allows edges to be created/queried
class QueryGraphManager {
public:
	QueryGraphManager(ClientContext &context) : relation_manager(context), context(context) {
	}

	//! manage relations and the logical operators they represent
	RelationManager relation_manager;

	//! A structure holding all the created JoinRelationSet objects
	JoinRelationSetManager set_manager;

	//! Extract the join relations, optimizing non-reoderable relations when encountered
	bool Build(LogicalOperator *op);

	unique_ptr<LogicalOperator> Reconstruct(unique_ptr<LogicalOperator> plan, JoinNode &node);

	const QueryGraph &GetQueryGraph() const;

	const vector<unique_ptr<FilterInfo>> &GetFilterBindings() const;

	//! Plan enumerator may not find a full plan and therefore will need to create cross
	//! products to create edges.
	void CreateQueryGraphCrossProduct(optional_ptr<JoinRelationSet> left, optional_ptr<JoinRelationSet> right);

private:
	ClientContext &context;

	vector<optional_ptr<LogicalOperator>> filter_operators;

	//! Filter information including the column_bindings that join filters
	//! used by the cardinality estimator to estimate distinct counts
	vector<unique_ptr<FilterInfo>> filters_and_bindings;

	QueryGraph query_graph;

	void GetColumnBinding(Expression &expression, ColumnBinding &binding);


	bool ExtractBindings(Expression &expression, unordered_set<idx_t> &bindings);

	void CreateHyperGraphEdges();

	GenerateJoinRelation GenerateJoins(vector<unique_ptr<LogicalOperator>> &extracted_relations, JoinNode &node);

	unique_ptr<LogicalOperator> RewritePlan(unique_ptr<LogicalOperator> plan, JoinNode &node);


};

} // namespace duckdb
