//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/tableref/bound_crossproductref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_tableref.hpp"

namespace duckdb {

//! Represents a cross product
class BoundCrossProductRef : public BoundTableRef {
public:
	BoundCrossProductRef() : BoundTableRef(TableReferenceType::CROSS_PRODUCT) {
	}

	//! The left hand side of the cross product
	unique_ptr<BoundTableRef> left;
	//! The right hand side of the cross product
	unique_ptr<BoundTableRef> right;
};
} // namespace duckdb
