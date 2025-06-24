#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class Index;

//! ConflictInfo contains information to match indexes to ON CONFLICT DO targets.
class ConflictInfo {
public:
	explicit ConflictInfo(const unordered_set<column_t> &column_ids, bool only_check_unique = true)
	    : column_ids(column_ids), only_check_unique(only_check_unique) {
	}

	const unordered_set<column_t> &column_ids;
	bool only_check_unique = true;

public:
	bool ConflictTargetMatches(Index &index) const;
	//! True, if the conflict info references the column ids of a single index, else false.
	bool SingleIndexTarget() const {
		return !column_ids.empty();
	}
};

} // namespace duckdb
