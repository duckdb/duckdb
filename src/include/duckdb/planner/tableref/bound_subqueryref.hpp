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
	static constexpr const TableReferenceType TYPE = TableReferenceType::SUBQUERY;

public:
	BoundSubqueryRef(shared_ptr<Binder> binder_p, unique_ptr<BoundQueryNode> subquery)
	    : BoundTableRef(TableReferenceType::SUBQUERY), binder(std::move(binder_p)), subquery(std::move(subquery)) {
	}

	//! The binder used to bind the subquery
	shared_ptr<Binder> binder;
	//! The bound subquery node
	unique_ptr<BoundQueryNode> subquery;
};
} // namespace duckdb
