//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/bound_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/parser/constraint.hpp"

namespace duckdb {
//! Bound equivalent of Constraint
class BoundConstraint {
public:
	BoundConstraint(ConstraintType type) : type(type){};
	virtual ~BoundConstraint() {
	}

	ConstraintType type;
};
} // namespace duckdb
