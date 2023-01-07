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
	BoundUniqueConstraint(vector<LogicalIndex> keys, logical_index_set_t key_set, bool is_primary_key)
	    : BoundConstraint(ConstraintType::UNIQUE), keys(move(keys)), key_set(move(key_set)),
	      is_primary_key(is_primary_key) {
#ifdef DEBUG
		D_ASSERT(keys.size() == key_set.size());
		for (auto &key : keys) {
			D_ASSERT(key_set.find(key) != key_set.end());
		}
#endif
	}

	//! The keys that define the unique constraint
	vector<LogicalIndex> keys;
	//! The same keys but stored as an unordered set
	logical_index_set_t key_set;
	//! Whether or not the unique constraint is a primary key
	bool is_primary_key;
};

} // namespace duckdb
