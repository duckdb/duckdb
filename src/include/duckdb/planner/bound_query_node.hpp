//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_query_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/order_type.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/planner/expression.hpp"

namespace duckdb {

struct BoundOrderByNode {
	BoundOrderByNode() {
	}
	BoundOrderByNode(OrderType type, unique_ptr<Expression> expression) : type(type), expression(move(expression)) {
	}

	OrderType type;
	unique_ptr<Expression> expression;
};

//! Bound equivalent of QueryNode
class BoundQueryNode {
public:
	BoundQueryNode(QueryNodeType type) : type(type) {
	}
	virtual ~BoundQueryNode() {
	}

	//! The type of the query node, either SetOperation or Select
	QueryNodeType type;
	//! DISTINCT or not
	bool select_distinct = false;
	//! List of order nodes
	vector<BoundOrderByNode> orders;
	//! List of target nodes for DISTINCT ON
	vector<unique_ptr<Expression>> target_distincts;
	//! LIMIT count
	int64_t limit = -1;
	//! OFFSET
	int64_t offset = -1;

	//! The names returned by this QueryNode.
	vector<string> names;
	//! The types returned by this QueryNode.
	vector<SQLType> types;

public:
	virtual idx_t GetRootIndex() = 0;
};

}; // namespace duckdb
