//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/query_node/bound_cte_node.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

class BoundCTENode : public BoundQueryNode {
public:
	static constexpr const QueryNodeType TYPE = QueryNodeType::CTE_NODE;

public:
	BoundCTENode() : BoundQueryNode(QueryNodeType::CTE_NODE) {
	}

	//! Keep track of the CTE name this node represents
	string ctename;

	//! The cte node
	BoundStatement query;
	//! The child node
	BoundStatement child;
	//! Index used by the set operation
	idx_t setop_index;
	//! The binder used by the query side of the CTE
	shared_ptr<Binder> query_binder;
	//! The binder used by the child side of the CTE
	shared_ptr<Binder> child_binder;

	CTEMaterialize materialized = CTEMaterialize::CTE_MATERIALIZE_DEFAULT;

public:
	idx_t GetRootIndex() override {
		return child.plan->GetRootIndex();
	}
};

} // namespace duckdb
