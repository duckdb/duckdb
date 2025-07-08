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

	void Finalize(DuckTransaction &transaction, DataTable &table);
	void Finalize(DataTable &table, LocalStorage &storage);

	const ConflictInfo &GetConflictInfo() const {
		return *conflict_info;
	}
	void FinishLookup();
	void SetMode(const ConflictManagerMode mode_p);

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
	bool HasConflicts(const idx_t i) const {
		return conflict_data[i].sel != nullptr;
	}
	idx_t ConflictCount(const idx_t i) const {
		if (!conflict_data[i].sel) {
			return 0;
		}
		return conflict_data[i].count;
	}
	Vector &GetRowIds(const idx_t i) {
		return *conflict_data[i].row_ids;
	}
	SelectionVector &GetSel(const idx_t i) {
		return *conflict_data[i].sel;
	}
	SelectionVector &GetInvertedSel(const idx_t i) {
		return *conflict_data[i].inverted_sel;
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

		void Insert(const idx_t index_in_chunk, const row_t row_id) {
			D_ASSERT(sel->get_index(index_in_chunk) == std::numeric_limits<sel_t>::max());
			sel->set_index(index_in_chunk, count);
			inverted_sel->set_index(count, index_in_chunk);
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
	unordered_set<idx_t> conflict_rows;
	bool finished = false;

	ConflictData &GetConflictData(const idx_t i) {
		auto &conflicts = conflict_data[i];
		if (!conflicts.sel) {
			D_ASSERT(!conflicts.row_ids);
			conflicts.sel = make_uniq<SelectionVector>(chunk_size);
			conflicts.inverted_sel = make_uniq<SelectionVector>(chunk_size);
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
		secondary_conflicts.Insert(index_in_chunk, row_id);
	}

	ConflictManagerMode mode;

	//! Indexes matching the conflict target.
	vector<reference<BoundIndex>> matched_indexes;
	//! Delete indexes matching the conflict target.
	vector<optional_ptr<BoundIndex>> matched_delete_indexes;
	//! All matched indexes by their name, which is their unique identifier.
	case_insensitive_set_t matched_index_names;
};

} // namespace duckdb
