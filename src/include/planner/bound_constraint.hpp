//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/bound_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "parser/constraint.hpp"

namespace duckdb {
//! Bound equivalent of Constraint
class BoundConstraint {
public:
	BoundConstraint(ConstraintType type) : type(type){};
	virtual ~BoundConstraint() {
	}

	ConstraintType type;
public:
	virtual unique_ptr<BoundConstraint> Copy() = 0;
};
} // namespace duckdb
