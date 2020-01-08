//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_subqueryref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_query_node.hpp"
#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

//! Represents a cross product
class BoundSubqueryRef : public BoundTableRef {
public:
	BoundSubqueryRef(unique_ptr<Binder> binder, unique_ptr<BoundQueryNode> subquery, index_t bind_index)
	    : BoundTableRef(TableReferenceType::SUBQUERY), binder(move(binder)), subquery(move(subquery)),
	      bind_index(bind_index) {
	}

	//! The binder used to bind the subquery
	unique_ptr<Binder> binder;
	//! The bound subquery node
	unique_ptr<BoundQueryNode> subquery;
	//! The index in the bind context
	index_t bind_index;
};
} // namespace duckdb
