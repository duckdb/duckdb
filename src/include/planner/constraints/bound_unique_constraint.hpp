//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/constraints/bound_unique_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/bound_constraint.hpp"

namespace duckdb {

class BoundUniqueConstraint : public BoundConstraint {
public:
	BoundUniqueConstraint(vector<index_t> keys, bool is_primary_key)
	    : BoundConstraint(ConstraintType::UNIQUE), keys(keys), is_primary_key(is_primary_key) {
	}

	//! The indexes to which the bound unique constraint pertains
	vector<index_t> keys;
	//! Whether or not the unique constraint is a primary key
	bool is_primary_key;

public:
	unique_ptr<BoundConstraint> Copy() override {
		return make_unique<BoundUniqueConstraint>(keys, is_primary_key);
	}
};

} // namespace duckdb
