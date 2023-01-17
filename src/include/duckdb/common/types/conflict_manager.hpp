#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class Index;
class ConflictInfo;

class ConflictManager {
public:
	ConflictManager(VerifyExistenceType lookup_type, idx_t input_size, ConflictInfo *conflict_info = nullptr);

public:
	void SetIndexCount(idx_t count);
	// These methods return a boolean indicating whether we should throw or not
	bool AddMiss(idx_t chunk_index, Index &index);
	bool AddHit(idx_t chunk_index, row_t row_id, Index &index);
	bool AddNull(idx_t chunk_index, Index &index);
	VerifyExistenceType LookupType() const;
	// This should be called before using the conflicts selection vector
	void Finalize();
	idx_t ConflictCount() const;
	const ManagedSelection &Conflicts() const;
	Vector &RowIds();
	const ConflictInfo &GetConflictInfo() const;
	void FinishLookup();

private:
	Vector &InternalRowIds();
	Vector &InternalIntermediate();
	ManagedSelection &InternalSelection();
	bool SingleIndexTarget() const;
	bool ShouldThrowOnConflict() const;
	bool ShouldIgnoreNulls() const;
	void AddConflictInternal(idx_t chunk_index, row_t row_id, Index &index);
	void AddOrUpdateConflict(row_t row_id, Index &index, bool matches_target);

private:
	VerifyExistenceType lookup_type;
	idx_t input_size;
	ConflictInfo *conflict_info;
	idx_t index_count;
	bool finalized = false;
	ManagedSelection conflicts;
	unique_ptr<Vector> row_ids;
	// Used to check if a given conflict is part of the conflict target or not
	unique_ptr<unordered_map<row_t, bool>> conflict_map;
	// Contains 'input_size' booleans, indicating if a given index in the input chunk has a conflict
	unique_ptr<Vector> intermediate_vector;
	// Mapping from chunk_index to row_id
	vector<row_t> row_id_map;
	// Whether we have already found the one conflict target we're interested in
	bool single_index_finished = false;
};

} // namespace duckdb
