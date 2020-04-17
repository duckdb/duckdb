//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/constraints/bound_unique_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/bound_constraint.hpp"

namespace duckdb {

class BoundUniqueConstraint : public BoundConstraint {
public:
	BoundUniqueConstraint(unordered_set<idx_t> keys, bool is_primary_key)
	    : BoundConstraint(ConstraintType::UNIQUE), keys(keys), is_primary_key(is_primary_key) {
	}

	//! The same keys but represented as an unordered set
	unordered_set<idx_t> keys;
	//! Whether or not the unique constraint is a primary key
	bool is_primary_key;
};

} // namespace duckdb
