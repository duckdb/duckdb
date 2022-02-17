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
	BoundForeignKeyConstraint(bool is_fk_table, string table, vector<idx_t> pk_keys, unordered_set<idx_t> pk_key_set,
	                          vector<idx_t> fk_keys, unordered_set<idx_t> fk_key_set)
	    : BoundConstraint(ConstraintType::FOREIGN_KEY), is_fk_table(is_fk_table), table(table), pk_keys(move(pk_keys)),
	      pk_key_set(move(pk_key_set)), fk_keys(move(fk_keys)), fk_key_set(move(fk_key_set)) {
#ifdef DEBUG
		D_ASSERT(pk_keys.size() == pk_key_set.size());
		for (auto &key : pk_keys) {
			D_ASSERT(pk_key_set.find(key) != pk_key_set.end());
		}
		D_ASSERT(fk_keys.size() == fk_key_set.size());
		for (auto &key : fk_keys) {
			D_ASSERT(fk_key_set.find(key) != fk_key_set.end());
		}
#endif
	}

	//! if this is true, this table has foreign keys.
	//! if this is false, this table is referenced table.
	bool is_fk_table;
	//! if is_fk_table is true, this is the referenced table.
	//! if is_fk_table is false, this is the table has foreign keys.
	string table;
	//! The primary keys of the referenced table
	vector<idx_t> pk_keys;
	//! The same keys but stored as an unordered set
	unordered_set<idx_t> pk_key_set;
	//! The keys that define the foreign key constraint
	vector<idx_t> fk_keys;
	//! The same keys but stored as an unordered set
	unordered_set<idx_t> fk_key_set;
};

} // namespace duckdb
