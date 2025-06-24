#include "duckdb/common/types/conflict_manager.hpp"

#include "duckdb/common/types/constraint_conflict_info.hpp"
#include "duckdb/execution/index/art/art.hpp"

namespace duckdb {

ConflictManager::ConflictManager(const VerifyExistenceType verify_existence_type, const idx_t chunk_size,
                                 optional_ptr<ConflictInfo> conflict_info)
    : verify_existence_type(verify_existence_type), chunk_size(chunk_size), conflict_info(conflict_info),
      conflicts(chunk_size, false), mode(ConflictManagerMode::THROW) {
}

ManagedSelection &ConflictManager::InternalSelection() {
	if (!conflicts.Initialized()) {
		conflicts.Initialize(chunk_size);
	}
	return conflicts;
}

Vector &ConflictManager::InternalRowIds() {
	if (!row_ids) {
		row_ids = make_uniq<Vector>(LogicalType::ROW_TYPE, chunk_size);
	}
	return *row_ids;
}

Vector &ConflictManager::InternalIntermediate() {
	if (!intermediate_vector) {
		intermediate_vector = make_uniq<Vector>(LogicalType::BOOLEAN, true, true, chunk_size);
	}
	return *intermediate_vector;
}

const ConflictInfo &ConflictManager::GetConflictInfo() const {
	D_ASSERT(conflict_info);
	return *conflict_info;
}

void ConflictManager::FinishLookup() {
	if (mode == ConflictManagerMode::THROW) {
		return;
	}
	if (!conflict_info->SingleIndexTarget()) {
		return;
	}
	if (conflicts.Count() != 0) {
		// We have recorded conflicts from the one index we're interested in
		// We set this so we don't duplicate the conflicts when there are duplicate indexes
		// that also match our conflict target
		single_index_finished = true;
	}
}

void ConflictManager::SetMode(const ConflictManagerMode mode_p) {
	// Scanning requires conflict_info.
	D_ASSERT(mode_p != ConflictManagerMode::SCAN || conflict_info != nullptr);
	mode = mode_p;
}

void ConflictManager::AddConflictInternal(const idx_t index_in_chunk, const row_t row_id) {
	D_ASSERT(mode == ConflictManagerMode::SCAN);
	D_ASSERT(!ShouldThrow(index_in_chunk));

	// Add the conflict to the conflict set.
	if (!conflict_set) {
		conflict_set = make_uniq<unordered_set<idx_t>>();
	}
	conflict_set->insert(index_in_chunk);

	if (conflict_info->SingleIndexTarget()) {
		// For identical indexes, we only record the conflicts of the first index,
		// because other identical index(ex) produce the exact conflicts.
		if (single_index_finished) {
			return;
		}

		// We don't need to merge conflicts of multiple indexes.
		// So we directly append the conflicts to the final result.
		auto &selection = InternalSelection();
		auto &internal_row_ids = InternalRowIds();
		auto data = FlatVector::GetData<row_t>(internal_row_ids);
		data[selection.Count()] = row_id;
		selection.Append(index_in_chunk);
		return;
	}

	auto &intermediate = InternalIntermediate();
	auto data = FlatVector::GetData<bool>(intermediate);
	// Mark this index in the chunk as producing a conflict
	data[index_in_chunk] = true;
	row_to_rowid[index_in_chunk] = row_id;
}

bool ConflictManager::IsConflict(LookupResultType type) {
	switch (type) {
	case LookupResultType::LOOKUP_NULL: {
		if (IgnoreNulls()) {
			return false;
		}
		// If nulls are not ignored, treat this as a hit instead
		return IsConflict(LookupResultType::LOOKUP_HIT);
	}
	case LookupResultType::LOOKUP_HIT: {
		return true;
	}
	case LookupResultType::LOOKUP_MISS: {
		// FIXME: If we record a miss as a conflict when the verify type is APPEND_FK, then we can simplify the checks
		// in VerifyForeignKeyConstraint This also means we should not record a hit as a conflict when the verify type
		// is APPEND_FK
		return false;
	}
	default: {
		throw NotImplementedException("Type not implemented for LookupResultType");
	}
	}
}

bool ConflictManager::AddHit(const idx_t index_in_chunk, const row_t row_id) {
	D_ASSERT(index_in_chunk < chunk_size);
	if (ShouldThrow(index_in_chunk)) {
		return true;
	}

	if (mode == ConflictManagerMode::THROW) {
		// When our mode is THROW, and the index is part of the previously scanned conflicts,
		// then we ignore the conflict instead
		D_ASSERT(!ShouldThrow(index_in_chunk));
		return false;
	}
	D_ASSERT(conflict_info);

	// Register the conflict and don't throw.
	AddConflictInternal(index_in_chunk, row_id);
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

bool ConflictManager::ShouldThrow(const idx_t index_in_chunk) const {
	if (mode == ConflictManagerMode::SCAN) {
		return false;
	}
	D_ASSERT(mode == ConflictManagerMode::THROW);

	// If we have already seen a conflict for this index, then we don't throw.
	if (conflict_set && conflict_set->count(index_in_chunk)) {
		return false;
	}
	return true;
}

Vector &ConflictManager::RowIds() {
	D_ASSERT(finalized);
	return *row_ids;
}

const ManagedSelection &ConflictManager::Conflicts() const {
	D_ASSERT(finalized);
	return conflicts;
}

idx_t ConflictManager::ConflictCount() const {
	return conflicts.Count();
}

void ConflictManager::AddIndex(BoundIndex &index, optional_ptr<BoundIndex> delete_index) {
	matched_indexes.push_back(index);
	matched_delete_indexes.push_back(delete_index);
	matched_index_names.insert(index.name);
}

bool ConflictManager::MatchedIndex(BoundIndex &index) {
	return matched_index_names.find(index.name) != matched_index_names.end();
}

const vector<reference<BoundIndex>> &ConflictManager::MatchedIndexes() const {
	return matched_indexes;
}

const vector<optional_ptr<BoundIndex>> &ConflictManager::MatchedDeleteIndexes() const {
	return matched_delete_indexes;
}

void ConflictManager::Finalize() {
	D_ASSERT(!finalized);
	if (conflict_info->SingleIndexTarget()) {
		// Selection vector has been directly populated already, no need to finalize
		finalized = true;
		return;
	}
	finalized = true;
	if (!intermediate_vector) {
		// No conflicts were found, we're done
		return;
	}
	auto &intermediate = InternalIntermediate();
	auto data = FlatVector::GetData<bool>(intermediate);
	auto &selection = InternalSelection();
	// Create the selection vector from the encountered conflicts
	for (idx_t i = 0; i < chunk_size; i++) {
		if (data[i]) {
			selection.Append(i);
		}
	}
	// Now create the row_ids Vector, aligned with the selection vector
	auto &internal_row_ids = InternalRowIds();
	auto row_id_data = FlatVector::GetData<row_t>(internal_row_ids);

	for (idx_t i = 0; i < selection.Count(); i++) {
		D_ASSERT(!row_to_rowid.empty());
		auto index = selection[i];
		D_ASSERT(index < chunk_size);
		auto row_id = row_to_rowid[index];
		row_id_data[i] = row_id;
	}
	intermediate_vector.reset();
}

} // namespace duckdb
