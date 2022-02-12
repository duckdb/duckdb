//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/constraints/bound_foreign_key_constraint.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/bound_constraint.hpp"

namespace duckdb {

class BoundForeignKeyConstraint : public BoundConstraint {
public:
	BoundForeignKeyConstraint(vector<idx_t> fk_keys, unordered_set<idx_t> fk_key_set, string pk_table,
	                          vector<idx_t> pk_keys, unordered_set<idx_t> pk_key_set)
	    : BoundConstraint(ConstraintType::FOREIGN_KEY), fk_keys(move(fk_keys)), fk_key_set(move(fk_key_set)),
	      pk_table(move(pk_table)), pk_keys(move(pk_keys)), pk_key_set(move(pk_key_set)) {
#ifdef DEBUG
		D_ASSERT(fk_keys.size() == fk_key_set.size());
		for (auto &key : fk_keys) {
			D_ASSERT(fk_key_set.find(key) != fk_key_set.end());
		}
		D_ASSERT(pk_keys.size() == pk_key_set.size());
		for (auto &key : pk_keys) {
			D_ASSERT(pk_key_set.find(key) != pk_key_set.end());
		}
#endif
	}

	//! The keys that define the foreign key constraint
	vector<idx_t> fk_keys;
	//! The same keys but stored as an unordered set
	unordered_set<idx_t> fk_key_set;
	//! referenced tabls's name
	string pk_table;
	//! The primary keys of the referenced table
	vector<idx_t> pk_keys;
	//! The same keys but stored as an unordered set
	unordered_set<idx_t> pk_key_set;
};

} // namespace duckdb
