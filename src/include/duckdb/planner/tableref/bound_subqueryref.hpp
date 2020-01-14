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
	BoundSubqueryRef(unique_ptr<Binder> binder, unique_ptr<BoundQueryNode> subquery)
	    : BoundTableRef(TableReferenceType::SUBQUERY), binder(move(binder)), subquery(move(subquery)) {
	}

	//! The binder used to bind the subquery
	unique_ptr<Binder> binder;
	//! The bound subquery node
	unique_ptr<BoundQueryNode> subquery;
};
} // namespace duckdb
