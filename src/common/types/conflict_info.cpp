#include "duckdb/common/types/constraint_conflict_info.hpp"

namespace duckdb {

bool ConflictInfo::ConflictTargetMatches(const bool is_unique, const unordered_set<column_t> &index_column_ids) const {
	if (only_check_unique && !is_unique) {
		// We only support ON CONFLICT for PRIMARY KEY/UNIQUE constraints.
		return false;
	}
	if (column_ids.empty()) {
		return true;
	}
	return column_ids == index_column_ids;
}

} // namespace duckdb
