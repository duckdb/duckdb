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
	static constexpr uint8_t FIRST = 0;
	static constexpr uint8_t SECOND = 1;

public:
	ConflictManager(const VerifyExistenceType lookup_type, const idx_t chunk_size,
	                optional_ptr<ConflictInfo> conflict_info = nullptr);

public:
	//! Returns true, if we need to throw, otherwise, adds the hit and returns false.
	bool AddHit(const idx_t index_in_chunk, const row_t row_id);
	//! Returns true, if we need to throw, otherwise, adds the NULL and returns false.
	bool AddNull(const idx_t index_in_chunk);

	//! Determine the row ID visible to the transaction for each index with two possible row IDs.
	void FinalizeGlobal(DuckTransaction &transaction, DataTable &table);
	//! Determine the row ID visible to the transaction for each index with two possible row IDs.
	void FinalizeLocal(DataTable &table, LocalStorage &storage);
	//! Determines if we are finished registering conflicts.
	void FinishLookup();

	//! Get the conflict information.
	const ConflictInfo &GetConflictInfo() const {
		return *conflict_info;
	}
	//! Sets the mode of the conflict manager.
	void SetMode(const ConflictManagerMode mode_p) {
		D_ASSERT(mode_p != ConflictManagerMode::SCAN || conflict_info != nullptr);
		mode = mode_p;
	}

	//! Adds an index and its respective delete_index.
	void AddIndex(BoundIndex &index, optional_ptr<BoundIndex> delete_index) {
		matching_indexes.push_back(index);
		matching_delete_indexes.push_back(delete_index);
		index_names.insert(index.name);
	}
	//! Returns true, if the index is in this conflict manager.
	bool IndexMatches(BoundIndex &index) {
		return index_names.find(index.name) != index_names.end();
	}
	//! Returns a reference to the matching indexes.
	const vector<reference<BoundIndex>> &MatchingIndexes() const {
		return matching_indexes;
	}
	//! Returns a reference to the matching delete indexes.
	const vector<optional_ptr<BoundIndex>> &MatchingDeleteIndexes() const {
		return matching_delete_indexes;
	}

	//! Returns the existence verification type.
	VerifyExistenceType GetVerifyExistenceType() const {
		return verify_existence_type;
	}
	//! Returns true, if there are any conflicts, else false.
	bool HasConflicts() const {
		return conflict_data[FIRST].sel != nullptr;
	}
	//! Returns the number of conflicts.
	idx_t ConflictCount() const {
		if (!conflict_data[FIRST].sel) {
			return 0;
		}
		return conflict_data[FIRST].count;
	}
	//! Returns a reference to the row IDs.
	//! Must be called after Finalize[Global|Local].
	Vector &GetRowIds() {
		D_ASSERT(!conflict_data[SECOND].sel);
		return *conflict_data[FIRST].row_ids;
	}
	//! Returns the first index in a chunk with a conflict.
	idx_t GetFirstIndex() const {
		return conflict_data[FIRST].inverted_sel->get_index(0);
	}
	//! Returns a reference to the inverted selection vector.
	//! Must be called after Finalize[Global|Local].
	SelectionVector &GetInvertedSel() {
		D_ASSERT(!conflict_data[SECOND].sel);
		return *conflict_data[FIRST].inverted_sel;
	}
	ValidityArray &GetValidityArray(const idx_t i) {
		return conflict_data[i].val_array;
	}

private:
	bool IsConflict(LookupResultType type);
	//! Returns true, if the conflict manager should throw an exception, else false.
	bool ShouldThrow(const idx_t index_in_chunk) const;

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
	ConflictManagerMode mode;

	//! Indexes matching the conflict target.
	vector<reference<BoundIndex>> matching_indexes;
	//! Delete indexes matching the conflict target.
	vector<optional_ptr<BoundIndex>> matching_delete_indexes;
	//! All matching indexes by their name (unique identifier).
	case_insensitive_set_t index_names;

	struct ConflictData {
		//! Links the conflicting rows to their row IDs (index_in_chunk -> count).
		unique_ptr<SelectionVector> sel;
		//! Inverted selection vector to slice the input chunk (count -> index_in_chunk).
		unique_ptr<SelectionVector> inverted_sel;
		//! Conflict count.
		idx_t count = 0;
		//! Row IDs.
		unique_ptr<Vector> row_ids;
		//! Optional row ID data.
		row_t *row_ids_data;
		//! Keeps track of the indexes in the chunk for which we've seen a conflict.
		//! True (valid) indicates a conflict.
		ValidityArray val_array;

		void Insert(const idx_t index_in_chunk, const row_t row_id) {
			D_ASSERT(!val_array.RowIsValid(index_in_chunk));
			sel->set_index(index_in_chunk, count);
			inverted_sel->set_index(count, index_in_chunk);
			val_array.SetValid(index_in_chunk);
			row_ids_data[count] = row_id;
			count++;
		}

		row_t GetRowId(const idx_t index_in_chunk) {
			auto idx = sel->get_index(index_in_chunk);
			return row_ids_data[idx];
		}

		void Reset() {
			count = 0;
			sel = nullptr;
			inverted_sel = nullptr;
			row_ids_data = nullptr;
			row_ids = nullptr;
		}
	};

	array<ConflictData, 2> conflict_data;
	//! Registers all conflicting rows in a data chunk.
	unordered_set<idx_t> conflict_rows;
	//! True, if we can skip recording any further conflicts.
	bool finished = false;

	ConflictData &GetConflictData(const idx_t i) {
		auto &conflicts = conflict_data[i];
		if (!conflicts.sel) {
			D_ASSERT(!conflicts.row_ids);
			conflicts.sel = make_uniq<SelectionVector>(chunk_size);
			conflicts.inverted_sel = make_uniq<SelectionVector>(chunk_size);
			conflicts.val_array.Initialize(chunk_size, false);
			conflicts.row_ids = make_uniq<Vector>(LogicalType::ROW_TYPE, chunk_size);
			conflicts.row_ids_data = FlatVector::GetData<row_t>(*conflicts.row_ids);
		}
		return conflicts;
	}

	void AddRowId(const idx_t index_in_chunk, const row_t row_id) {
		auto elem = conflict_rows.find(index_in_chunk);
		auto &primary_conflicts = GetConflictData(0);

		if (elem == conflict_rows.end()) {
			// We have not yet seen this conflict: insert.
			conflict_rows.insert(index_in_chunk);
			primary_conflicts.Insert(index_in_chunk, row_id);
			return;
		}

		if (primary_conflicts.GetRowId(index_in_chunk) == row_id) {
			// We have already seen this conflict.
			return;
		}

		// We have seen a conflict for this index, but with a different row ID.
		auto &secondary_conflicts = GetConflictData(1);
		if (secondary_conflicts.val_array.RowIsValid(index_in_chunk) &&
		    secondary_conflicts.GetRowId(index_in_chunk) == row_id) {
			// We have already seen this conflict.
			return;
		}
		secondary_conflicts.Insert(index_in_chunk, row_id);
	}
};

} // namespace duckdb
