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
#include "duckdb/common/index_map.hpp"

namespace duckdb {

class BoundForeignKeyConstraint : public BoundConstraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::FOREIGN_KEY;

public:
	BoundForeignKeyConstraint(ForeignKeyInfo info_p, physical_index_set_t pk_key_set_p,
	                          physical_index_set_t fk_key_set_p)
	    : BoundConstraint(ConstraintType::FOREIGN_KEY), info(std::move(info_p)), pk_key_set(std::move(pk_key_set_p)),
	      fk_key_set(std::move(fk_key_set_p)) {
#ifdef DEBUG
		D_ASSERT(info.pk_keys.size() == pk_key_set.size());
		for (auto &key : info.pk_keys) {
			D_ASSERT(pk_key_set.find(key) != pk_key_set.end());
		}
		D_ASSERT(info.fk_keys.size() == fk_key_set.size());
		for (auto &key : info.fk_keys) {
			D_ASSERT(fk_key_set.find(key) != fk_key_set.end());
		}
#endif
	}

	ForeignKeyInfo info;
	//! The same keys but stored as an unordered set
	physical_index_set_t pk_key_set;
	//! The same keys but stored as an unordered set
	physical_index_set_t fk_key_set;
};

} // namespace duckdb
