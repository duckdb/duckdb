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
	finished = HasConflicts();
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
	if (finished) {
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

void ConflictManager::FinalizeGlobal(DuckTransaction &transaction, DataTable &table) {
	if (!conflict_data[SECOND].sel) {
		return;
	}

	for (idx_t i = 0; i < conflict_data[SECOND].count; i++) {
		auto row_id = conflict_data[SECOND].row_ids_data[i];
		if (table.CanFetch(transaction, row_id)) {
			// Replace the primary row ID with the secondary row ID.
			auto index_in_chunk = conflict_data[SECOND].inverted_sel->get_index(i);
			auto count = conflict_data[FIRST].sel->get_index(index_in_chunk);
			conflict_data[FIRST].row_ids_data[count] = row_id;
		}
	}

	conflict_data[SECOND].Reset();
}

void ConflictManager::FinalizeLocal(DataTable &table, LocalStorage &storage) {
	if (!conflict_data[SECOND].sel) {
		return;
	}

	for (idx_t i = 0; i < conflict_data[SECOND].count; i++) {
		auto row_id = conflict_data[SECOND].row_ids_data[i];
		if (storage.CanFetch(table, row_id)) {
			// Replace the primary row ID with the secondary row ID.
			auto index_in_chunk = conflict_data[SECOND].inverted_sel->get_index(i);
			auto count = conflict_data[FIRST].sel->get_index(index_in_chunk);
			conflict_data[FIRST].row_ids_data[count] = row_id;
		}
	}

	conflict_data[SECOND].Reset();
}

bool ConflictManager::ShouldThrow(const idx_t index_in_chunk) const {
	// Never throw on scans.
	if (mode == ConflictManagerMode::SCAN) {
		return false;
	}

	// If we have already seen a conflict for this index, then we don't throw.
	D_ASSERT(mode == ConflictManagerMode::THROW);
	return conflict_rows.find(index_in_chunk) == conflict_rows.end();
}

} // namespace duckdb
