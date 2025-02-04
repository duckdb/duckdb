//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/conflict_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Index;
class ConflictInfo;

enum class ConflictManagerMode : uint8_t {
	SCAN, // gather conflicts without throwing
	THROW // throw on the conflicts that were not found during the scan
};

enum class LookupResultType : uint8_t { LOOKUP_MISS, LOOKUP_HIT, LOOKUP_NULL };

class ConflictManager {
public:
	ConflictManager(VerifyExistenceType lookup_type, idx_t input_size,
	                optional_ptr<ConflictInfo> conflict_info = nullptr);

public:
	// These methods return a boolean indicating whether we should throw or not
	bool AddMiss(idx_t chunk_index);
	bool AddHit(idx_t chunk_index, row_t row_id);
	bool AddNull(idx_t chunk_index);
	VerifyExistenceType LookupType() const;
	// This should be called before using the conflicts selection vector
	void Finalize();

	Vector &RowIds();
	const ConflictInfo &GetConflictInfo() const;
	void FinishLookup();
	void SetMode(ConflictManagerMode mode);

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

private:
	bool IsConflict(LookupResultType type);
	const unordered_set<idx_t> &InternalConflictSet() const;
	Vector &InternalRowIds();
	Vector &InternalIntermediate();
	ManagedSelection &InternalSelection();
	bool SingleIndexTarget() const;
	bool ShouldThrow(idx_t chunk_index) const;
	bool ShouldIgnoreNulls() const;
	void AddConflictInternal(idx_t chunk_index, row_t row_id);
	void AddToConflictSet(idx_t chunk_index);

private:
	VerifyExistenceType lookup_type;
	idx_t input_size;
	optional_ptr<ConflictInfo> conflict_info;
	bool finalized = false;
	ManagedSelection conflicts;
	unique_ptr<Vector> row_ids;
	// Used to check if a given conflict is part of the conflict target or not
	unique_ptr<unordered_set<idx_t>> conflict_set;
	// Contains 'input_size' booleans, indicating if a given index in the input chunk has a conflict
	unique_ptr<Vector> intermediate_vector;
	// Mapping from chunk_index to row_id
	vector<row_t> row_id_map;
	// Whether we have already found the one conflict target we're interested in
	bool single_index_finished = false;
	ConflictManagerMode mode;

	//! Indexes matching the conflict target.
	vector<reference<BoundIndex>> matched_indexes;
	//! Delete indexes matching the conflict target.
	vector<optional_ptr<BoundIndex>> matched_delete_indexes;
	//! All matched indexes by their name, which is their unique identifier.
	case_insensitive_set_t matched_index_names;
};

} // namespace duckdb
