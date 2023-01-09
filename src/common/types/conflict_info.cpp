#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

bool ConflictInfo::ConflictTargetMatches(Index &index) const {
	if (!index.IsUnique()) {
		// We only support checking ON CONFLICT for Unique/Primary key constraints
		return false;
	}
	if (column_ids.empty()) {
		return true;
	}
	if (column_ids.size() != index.column_id_set.size()) {
		// All targets need to match, not only partially
		return false;
	}
	for (auto &id : column_ids) {
		if (!index.column_id_set.count(id)) {
			return false;
		}
	}
	return true;
}

} // namespace duckdb
