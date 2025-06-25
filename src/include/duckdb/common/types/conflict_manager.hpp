//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/conflict_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

class Index;
class ConflictInfo;

enum class ConflictManagerMode : uint8_t {
	SCAN, // Gather conflicts without throwing a constraint exception.
	THROW // Throw on the conflicts that were not found during the scan.
};

enum class LookupResultType : uint8_t { LOOKUP_MISS, LOOKUP_HIT, LOOKUP_NULL };

//! The conflict manager tracks conflicts during constraint verification.
//! It decides when/if to throw, and records conflicts to be used later in case
//! of ON CONFLICT DO. The conflict manager operates on a per-chunk basis,
//! i.e., we construct a new conflict manager per incoming chunk.
//! On conflict manager can gather conflicts across multiple indexes, if
//! they match the ON CONFLICT DO target.
class ConflictManager {
public:
	ConflictManager(const VerifyExistenceType lookup_type, const idx_t chunk_size,
	                optional_ptr<ConflictInfo> conflict_info = nullptr);

public:
	// These methods return a boolean indicating whether we should throw or not
	bool AddHit(const idx_t index_in_chunk, const row_t row_id);
	bool AddNull(const idx_t index_in_chunk);
	// This should be called before using the conflicts selection vector
	void Finalize();

	Vector &RowIds();
	const ConflictInfo &GetConflictInfo() const;
	void FinishLookup();
	void SetMode(const ConflictManagerMode mode_p);

	//! Returns a reference to all conflicts in this conflict manager.
	const ManagedSelection &Conflicts() const;
	//! Returns the number of conflicts in this conflict manager.
	idx_t ConflictCount() const;
	//! Adds an index and its respective delete_index to the conflict manager's matches.
	void AddIndex(BoundIndex &index, optional_ptr<BoundIndex> delete_index);
	//! Returns true, if the index is in this conflict manager.
	bool MatchedIndex(BoundIndex &index);
	//! Returns a reference to the matched indexes.
	const vector<reference<BoundIndex>> &MatchedIndexes() const;
	//! Returns a reference to the matched delete indexes.
	const vector<optional_ptr<BoundIndex>> &MatchedDeleteIndexes() const;

	VerifyExistenceType GetVerifyExistenceType() const {
		return verify_existence_type;
	}

private:
	bool IsConflict(LookupResultType type);
	Vector &InternalRowIds();
	Vector &InternalIntermediate();
	ManagedSelection &InternalSelection();
	//! Returns true, if the conflict manager should throw an exception, else false.
	bool ShouldThrow(const idx_t index_in_chunk) const;
	void AddConflictInternal(const idx_t index_in_chunk, const row_t row_id);

	//! Returns true, if we ignore NULLs, else false.
	bool IgnoreNulls() const {
		switch (verify_existence_type) {
		case VerifyExistenceType::APPEND:
			return true;
		case VerifyExistenceType::APPEND_FK:
			return false;
		case VerifyExistenceType::DELETE_FK:
			return true;
		default:
			throw InternalException("Type not implemented for VerifyExistenceType");
		}
	}

private:
	//! The type of constraint verification for which we're using the conflict manager.
	VerifyExistenceType verify_existence_type;
	//! The count of the data chunk for which we've created the conflict manager.
	idx_t chunk_size;
	//! Optional information to match indexes to the conflict target.
	optional_ptr<ConflictInfo> conflict_info;

	ManagedSelection conflicts;
	ConflictManagerMode mode;

	bool finalized = false;
	unique_ptr<Vector> row_ids;
	// Used to check if a given conflict is part of the conflict target or not
	unique_ptr<unordered_set<idx_t>> conflict_set;
	// Contains 'input_size' booleans, indicating if a given index in the input chunk has a conflict
	unique_ptr<Vector> intermediate_vector;
	//! Each row in the data chunk has a key. With that key, we can look up the row ID
	//! in the index. row_to_rowid maps the row to its row ID.
	unordered_map<idx_t, row_t> row_to_rowid;
	// Whether we have already found the one conflict target we're interested in
	bool single_index_finished = false;

	//! Indexes matching the conflict target.
	vector<reference<BoundIndex>> matched_indexes;
	//! Delete indexes matching the conflict target.
	vector<optional_ptr<BoundIndex>> matched_delete_indexes;
	//! All matched indexes by their name, which is their unique identifier.
	case_insensitive_set_t matched_index_names;
};

} // namespace duckdb
