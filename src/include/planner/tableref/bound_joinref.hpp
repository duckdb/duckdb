//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/tableref/bound_joinref.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/enums/join_type.hpp"
#include "planner/bound_tableref.hpp"
#include "planner/expression.hpp"

namespace duckdb {

//! Represents a join
class BoundJoinRef : public BoundTableRef {
public:
	BoundJoinRef() : BoundTableRef(TableReferenceType::JOIN) {
	}

	//! The left hand side of the join
	unique_ptr<BoundTableRef> left;
	//! The right hand side of the join
	unique_ptr<BoundTableRef> right;
	//! The join condition
	unique_ptr<Expression> condition;
	//! The join type
	JoinType type;
};
} // namespace duckdb
