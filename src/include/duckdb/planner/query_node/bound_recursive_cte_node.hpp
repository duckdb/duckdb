//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/query_node/bound_recursive_cte_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

//! Bound equivalent of SetOperationNode
class BoundRecursiveCTENode : public BoundQueryNode {
public:
	BoundRecursiveCTENode() : BoundQueryNode(QueryNodeType::RECURSIVE_CTE_NODE) {
	}

	//! Keep track of the CTE name this node represents
	string ctename;

	bool union_all;
	//! The left side of the set operation
	unique_ptr<BoundQueryNode> left;
	//! The right side of the set operation
	unique_ptr<BoundQueryNode> right;

	//! Index used by the set operation
	idx_t setop_index;
	//! The binder used by the left side of the set operation
	unique_ptr<Binder> left_binder;
	//! The binder used by the right side of the set operation
	unique_ptr<Binder> right_binder;

public:
	idx_t GetRootIndex() override {
		return setop_index;
	}
};

}; // namespace duckdb
