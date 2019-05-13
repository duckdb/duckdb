//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/constraints/bound_not_null_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_constraint.hpp"

namespace duckdb {

class BoundUniqueConstraint : public BoundConstraint {
public:
	BoundUniqueConstraint(uint64_t index)
	    : BoundConstraint(ConstraintType::NOT_NULL), index(index) {
	}

	//! Column index this constraint pertains to
	uint64_t index;
public:
	unique_ptr<BoundConstraint> Copy() override {
		return make_unique<BoundUniqueConstraint>(...);
	}
};

} // namespace duckdb
