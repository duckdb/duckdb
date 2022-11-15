//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_crossproductref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

//! Represents a cross product
class BoundCrossProductRef : public BoundTableRef {
public:
	BoundCrossProductRef() : BoundTableRef(TableReferenceType::CROSS_PRODUCT) {
	}

	//! The binder used to bind the LHS of the cross product
	shared_ptr<Binder> left_binder;
	//! The binder used to bind the RHS of the cross product
	shared_ptr<Binder> right_binder;
	//! The left hand side of the cross product
	unique_ptr<BoundTableRef> left;
	//! The right hand side of the cross product
	unique_ptr<BoundTableRef> right;
	//! The correlated columns of the right-side with the left-side
	vector<CorrelatedColumnInfo> correlated_columns;
};
} // namespace duckdb
