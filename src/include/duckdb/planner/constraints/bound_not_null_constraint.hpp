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
	explicit BoundNotNullConstraint(column_t index) : BoundConstraint(ConstraintType::NOT_NULL), index(index) {
	}

	//! Column index this constraint pertains to
	storage_t index;
};

} // namespace duckdb
