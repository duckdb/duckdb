#include "duckdb/common/types/conflict_manager.hpp"

#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

ConflictManager::ConflictManager(const VerifyExistenceType verify_existence_type, const idx_t chunk_size,
                                 optional_ptr<ConflictInfo> conflict_info)
    : verify_existence_type(verify_existence_type), chunk_size(chunk_size), conflict_info(conflict_info),
      mode(ConflictManagerMode::THROW) {
}

void ConflictManager::FinishLookup() {
	if (mode == ConflictManagerMode::THROW) {
		return;
	}
	if (!conflict_info->SingleIndexTarget()) {
		return;
	}

	// We recorded conflicts of one of the indexes.
	// We can skip any duplicate indexes matching the same conflict target.
	finished = HasConflicts(0);
}

void ConflictManager::SetMode(const ConflictManagerMode mode_p) {
	// Scanning requires conflict_info.
	D_ASSERT(mode_p != ConflictManagerMode::SCAN || conflict_info != nullptr);
	mode = mode_p;
}

bool ConflictManager::IsConflict(LookupResultType type) {
	switch (type) {
	case LookupResultType::LOOKUP_NULL:
		if (IgnoreNulls()) {
			return false;
		}
		// If we ignore NULL, then we treat this as a hit.
		return IsConflict(LookupResultType::LOOKUP_HIT);
	case LookupResultType::LOOKUP_HIT:
		return true;
	case LookupResultType::LOOKUP_MISS:
		// FIXME: If we record a miss as a conflict when the verify type is APPEND_FK, then we can simplify the checks
		// in VerifyForeignKeyConstraint This also means we should not record a hit as a conflict when the verify type
		// is APPEND_FK
		return false;
	default:
		throw NotImplementedException("Type not implemented for LookupResultType");
	}
}

bool ConflictManager::AddHit(const idx_t index_in_chunk, const row_t row_id) {
	D_ASSERT(index_in_chunk < chunk_size);
	if (ShouldThrow(index_in_chunk)) {
		return true;
	}

	if (mode == ConflictManagerMode::THROW) {
		// On THROW, and if the index is part of the previously scanned conflicts,
		// we ignore the conflict.
		D_ASSERT(!ShouldThrow(index_in_chunk));
		return false;
	}
	D_ASSERT(conflict_info);

	// Add the conflict and don't throw.
	D_ASSERT(mode == ConflictManagerMode::SCAN);
	D_ASSERT(!ShouldThrow(index_in_chunk));
	if (finished) { // TODO: correct here? do we need this?
		return false;
	}
	AddRowId(index_in_chunk, row_id);
	return false;
}

bool ConflictManager::AddNull(const idx_t index_in_chunk) {
	D_ASSERT(index_in_chunk < chunk_size);
	if (!IsConflict(LookupResultType::LOOKUP_NULL)) {
		return false;
	}
	auto row_id = static_cast<row_t>(DConstants::INVALID_INDEX);
	return AddHit(index_in_chunk, row_id);
}

void ConflictManager::Finalize(DuckTransaction &transaction, DataTable &table) {
	if (!HasConflicts(1)) {
		return;
	}

	auto &primary_sel = *conflict_data[0].sel;
	auto primary_row_ids_data = conflict_data[0].row_ids_data;

	auto secondary_count = conflict_data[1].count;
	auto &secondary_inverted_sel = *conflict_data[1].inverted_sel;
	auto secondary_row_ids_data = conflict_data[1].row_ids_data;

	for (idx_t i = 0; i < secondary_count; i++) {
		auto secondary_row_id = secondary_row_ids_data[i];

		// Try to fetch the row ID. True, if the transaction sees the row ID.
		if (table.CanFetch(transaction, secondary_row_id)) {
			// Replace the primary row ID with the secondary row ID.
			auto index_in_chunk = secondary_inverted_sel.get_index(i);
			auto primary_count = primary_sel.get_index(index_in_chunk);
			primary_row_ids_data[primary_count] = secondary_row_id;
		}
	}

	conflict_data[1].Reset();
}

// TODO: unify
void ConflictManager::Finalize(DataTable &table, LocalStorage &storage) {
	if (!HasConflicts(1)) {
		return;
	}

	auto &primary_sel = *conflict_data[0].sel;
	auto primary_row_ids_data = conflict_data[0].row_ids_data;

	auto secondary_count = conflict_data[1].count;
	auto &secondary_inverted_sel = *conflict_data[1].inverted_sel;
	auto secondary_row_ids_data = conflict_data[1].row_ids_data;

	for (idx_t i = 0; i < secondary_count; i++) {
		auto secondary_row_id = secondary_row_ids_data[i];

		// Try to fetch the row ID. True, if the transaction sees the row ID.
		if (storage.CanFetch(table, secondary_row_id)) {
			// Replace the primary row ID with the secondary row ID.
			auto index_in_chunk = secondary_inverted_sel.get_index(i);
			auto primary_count = primary_sel.get_index(index_in_chunk);
			primary_row_ids_data[primary_count] = secondary_row_id;
		}
	}

	conflict_data[1].Reset();
}

bool ConflictManager::ShouldThrow(const idx_t index_in_chunk) const {
	// Never throw on scans.
	if (mode == ConflictManagerMode::SCAN) {
		return false;
	}

	// If we have already seen a conflict for this index, then we don't throw.
	D_ASSERT(mode == ConflictManagerMode::THROW);
	if (conflict_rows.find(index_in_chunk) != conflict_rows.end()) {
		return false;
	}

	return true;
}

void ConflictManager::AddIndex(BoundIndex &index, optional_ptr<BoundIndex> delete_index) {
	matched_indexes.push_back(index);
	matched_delete_indexes.push_back(delete_index);
	matched_index_names.insert(index.name);
}

// TODO: better name?
bool ConflictManager::MatchedIndex(BoundIndex &index) {
	return matched_index_names.find(index.name) != matched_index_names.end();
}

// TODO: better name?
const vector<reference<BoundIndex>> &ConflictManager::MatchedIndexes() const {
	return matched_indexes;
}

// TODO: better name?
const vector<optional_ptr<BoundIndex>> &ConflictManager::MatchedDeleteIndexes() const {
	return matched_delete_indexes;
}

} // namespace duckdb
