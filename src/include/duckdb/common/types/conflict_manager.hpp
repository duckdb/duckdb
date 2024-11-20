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
	idx_t ConflictCount() const;
	const ManagedSelection &Conflicts() const;
	Vector &RowIds();
	const ConflictInfo &GetConflictInfo() const;
	void FinishLookup();
	void SetMode(ConflictManagerMode mode);
	void AddIndex(BoundIndex &index);
	bool MatchedIndex(BoundIndex &index);
	const unordered_set<BoundIndex *> &MatchedIndexes() const;

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
	unordered_set<BoundIndex *> matched_indexes;
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
};

} // namespace duckdb
