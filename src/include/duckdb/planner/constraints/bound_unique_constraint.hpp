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
#include "duckdb/common/index_map.hpp"

namespace duckdb {

class BoundUniqueConstraint : public BoundConstraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::UNIQUE;

public:
	BoundUniqueConstraint(vector<PhysicalIndex> keys_p, physical_index_set_t key_set_p, const bool is_primary_key)
	    : BoundConstraint(ConstraintType::UNIQUE), keys(std::move(keys_p)), key_set(std::move(key_set_p)),
	      is_primary_key(is_primary_key) {
#ifdef DEBUG
		D_ASSERT(keys.size() == key_set.size());
		for (auto &key : keys) {
			D_ASSERT(key_set.find(key) != key_set.end());
		}
#endif
	}

	//! The keys that define the unique constraint.
	vector<PhysicalIndex> keys;
	//! The same keys but stored as an unordered set.
	physical_index_set_t key_set;
	//! Whether this is a PRIMARY KEY constraint, or a UNIQUE constraint.
	bool is_primary_key;

public:
	unique_ptr<BoundConstraint> Copy() const override {
		return make_uniq<BoundUniqueConstraint>(keys, key_set, is_primary_key);
	}
};

} // namespace duckdb
