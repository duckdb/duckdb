#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {

bool ConflictInfo::ConflictTargetMatches(Index &index) const {
	if (!index.IsUnique()) {
		// We only support checking ON CONFLICT for Unique/Primary key constraints
		return false;
	}
	if (!column_ids.empty()) {
		for (auto &id : column_ids) {
			if (index.column_id_set.count(id)) {
				return true;
			}
		}
		return false;
	}
	return true;
}

} // namespace duckdb
