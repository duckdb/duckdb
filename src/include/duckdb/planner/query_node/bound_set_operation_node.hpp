//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/query_node/bound_set_operation_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/set_operation_type.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

//! Bound equivalent of SetOperationNode
class BoundSetOperationNode : public BoundQueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::SET_OPERATION_NODE;

public:
	BoundSetOperationNode() : BoundQueryNode(QueryNodeType::SET_OPERATION_NODE) {
	}

	//! The type of set operation
	SetOperationType setop_type = SetOperationType::NONE;
	//! whether the ALL modifier was used or not
	bool setop_all = false;
	//! The left side of the set operation
	unique_ptr<BoundQueryNode> left;
	//! The right side of the set operation
	unique_ptr<BoundQueryNode> right;

	//! Index used by the set operation
	idx_t setop_index;
	//! The binder used by the left side of the set operation
	shared_ptr<Binder> left_binder;
	//! The binder used by the right side of the set operation
	shared_ptr<Binder> right_binder;

	//! Exprs used by the UNION BY NAME opeartons to add a new projection
	vector<unique_ptr<Expression>> left_reorder_exprs;
	vector<unique_ptr<Expression>> right_reorder_exprs;

	//! The exprs of the child node may be rearranged(UNION BY NAME),
	//! this vector records the new index of the expression after rearrangement
	//! used by GatherAlias(...) function to create new reorder index
	vector<idx_t> left_reorder_idx;
	vector<idx_t> right_reorder_idx;

public:
	idx_t GetRootIndex() override {
		return setop_index;
	}
};

} // namespace duckdb
