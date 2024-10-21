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
#include "duckdb/storage/index_storage_info.hpp"

namespace duckdb {

class BoundUniqueConstraint : public BoundConstraint {
public:
	static constexpr const ConstraintType TYPE = ConstraintType::UNIQUE;

public:
	BoundUniqueConstraint(vector<PhysicalIndex> keys, physical_index_set_t key_set, bool is_primary_key,
	                      IndexStorageInfo &info)
	    : BoundConstraint(ConstraintType::UNIQUE), keys(std::move(keys)), key_set(std::move(key_set)),
	      is_primary_key(is_primary_key), info(info) {
#ifdef DEBUG
		D_ASSERT(this->keys.size() == this->key_set.size());
		for (auto &key : this->keys) {
			D_ASSERT(this->key_set.find(key) != this->key_set.end());
		}
#endif
	}

	//! The keys that define the unique constraint
	vector<PhysicalIndex> keys;
	//! The same keys but stored as an unordered set
	physical_index_set_t key_set;
	//! Whether or not the unique constraint is a primary key
	bool is_primary_key;
	//! Optional index storage info after WAL replay.
	IndexStorageInfo info;
};

} // namespace duckdb
