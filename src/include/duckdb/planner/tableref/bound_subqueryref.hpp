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

namespace duckdb {

//! Represents a cross product
class BoundSubqueryRef {
public:
	BoundSubqueryRef(shared_ptr<Binder> binder_p, BoundStatement subquery)
	    : binder(std::move(binder_p)), subquery(std::move(subquery)) {
	}

	//! The binder used to bind the subquery
	shared_ptr<Binder> binder;
	//! The bound subquery node (if any)
	BoundStatement subquery;
};
} // namespace duckdb
