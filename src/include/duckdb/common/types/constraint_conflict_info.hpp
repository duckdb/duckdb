#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

class Index;

struct UniqueConstraintConflictInfo {
public:
	UniqueConstraintConflictInfo(idx_t count) : matches(count), row_ids(LogicalType::ROW_TYPE, count), count(count) {
	}

public:
	// Find the first index that is not null, and did not find a match
	static idx_t FirstMissingMatch(const ManagedSelection &matches) {
		idx_t match_idx = 0;

		for (idx_t i = 0; i < matches.Size(); i++) {
			auto match = matches.IndexMapsToLocation(match_idx, i);
			match_idx += match;
			if (!match) {
				// This index is missing in the matches vector
				return i;
			}
		}
		return DConstants::INVALID_INDEX;
	}

public:
	// TODO: this can be optimized, if we just create a vector the size of the input chunk,
	// and update which indices have conflicts, then we can construct a selection vector at the end
	// merging these selection vectors and rowid vectors like this is likely more expensive

	void AddConflicts(UniqueConstraintConflictInfo &info) {
		D_ASSERT(info.count == count);
		if (info.matches.Count() == 0) {
			// No additional matches (conflicts) to record
			return;
		}
		// Merge info into this
		idx_t new_conflicts = NewConflicts(info);
		if (new_conflicts == 0) {
			// No need to do anything, 'info' either has no conflicts, or shares them with 'this'
			return;
		}

		// We need to create a new selection vector and row_ids vector that contain conflicts from both
		ManagedSelection new_sel(count);
		Vector new_rowids(LogicalType::ROW_TYPE, count);
		auto new_rowid_data = FlatVector::GetData<row_t>(new_rowids);

		auto lhs_rowid_data = FlatVector::GetData<row_t>(row_ids);
		auto rhs_rowid_data = FlatVector::GetData<row_t>(info.row_ids);
		idx_t lhs_idx = 0;
		idx_t rhs_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			auto in_this = matches.IndexMapsToLocation(lhs_idx, i);
			auto in_other = info.matches.IndexMapsToLocation(rhs_idx, i);
			if (!in_this && !in_other) {
				continue;
			}

			// Conflict found, add from this or other
			row_t row_id = DConstants::INVALID_INDEX;
			if (in_this) {
				row_id = lhs_rowid_data[lhs_idx];
				// This conflict should either not be in the other
				// or their rowid should be the same
				D_ASSERT(!in_other || (rhs_rowid_data[rhs_idx] == row_id));
			} else {
				row_id = rhs_rowid_data[rhs_idx];
			}
			new_rowid_data[new_sel.Count()] = row_id;
			new_sel.Append(i);

			lhs_idx += in_this;
			rhs_idx += in_other;
		}
		matches = move(new_sel);
		row_ids.Reference(new_rowids);
	}

private:
	idx_t NewConflicts(UniqueConstraintConflictInfo &other) {
		D_ASSERT(other.count == count);
		idx_t new_conflicts = 0;

		idx_t lhs_idx = 0;
		idx_t rhs_idx = 0;
		for (idx_t i = 0; i < count; i++) {
			auto in_this = matches.IndexMapsToLocation(lhs_idx, i);
			lhs_idx += in_this;
			auto in_other = other.matches.IndexMapsToLocation(rhs_idx, i);
			rhs_idx += in_other;
			if (in_other && !in_this) {
				new_conflicts++;
			}
			if (rhs_idx >= other.matches.Count()) {
				// There are no more conflicts in other
				break;
			}
		}
		return new_conflicts;
	}

public:
	// This stores the indices for the matches
	ManagedSelection matches;
	// Contains the row_ids of the matching values
	Vector row_ids;
	// The total amount of values
	idx_t count;
};

class ConflictInfo {
public:
	ConflictInfo(idx_t count, const unordered_set<column_t> &column_ids)
	    : constraint_conflicts(count), column_ids(column_ids) {
	}
	UniqueConstraintConflictInfo constraint_conflicts;
	const unordered_set<column_t> &column_ids;

public:
	bool ConflictTargetMatches(Index &index) const;
	void VerifyAllConflictsMeetCondition() const;
};

} // namespace duckdb
