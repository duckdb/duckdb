#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class Index;

class ConflictInfo {
public:
	ConflictInfo(const unordered_set<column_t> &column_ids, bool only_check_unique = true)
	    : column_ids(column_ids), only_check_unique(only_check_unique) {
	}
	const unordered_set<column_t> &column_ids;

public:
	bool ConflictTargetMatches(Index &index) const;
	void VerifyAllConflictsMeetCondition() const;

public:
	bool only_check_unique = true;
};

} // namespace duckdb
