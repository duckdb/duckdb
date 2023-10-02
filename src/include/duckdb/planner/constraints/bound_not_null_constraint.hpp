//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/constraints/bound_not_null_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/bound_constraint.hpp"

namespace duckdb {

class BoundNotNullConstraint : public BoundConstraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::NOT_NULL;

public:
	explicit BoundNotNullConstraint(PhysicalIndex index) : BoundConstraint(ConstraintType::NOT_NULL), index(index) {
	}

	//! Column index this constraint pertains to
	PhysicalIndex index;
};

} // namespace duckdb
