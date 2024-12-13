#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"

namespace duckdb {

ConflictManager::ConflictManager(VerifyExistenceType lookup_type, idx_t input_size,
                                 optional_ptr<ConflictInfo> conflict_info)
    : lookup_type(lookup_type), input_size(input_size), conflict_info(conflict_info), conflicts(input_size, false),
      mode(ConflictManagerMode::THROW) {
}

ManagedSelection &ConflictManager::InternalSelection() {
	if (!conflicts.Initialized()) {
		conflicts.Initialize(input_size);
	}
	return conflicts;
}

const unordered_set<idx_t> &ConflictManager::InternalConflictSet() const {
	D_ASSERT(conflict_set);
	return *conflict_set;
}

Vector &ConflictManager::InternalRowIds() {
	if (!row_ids) {
		row_ids = make_uniq<Vector>(LogicalType::ROW_TYPE, input_size);
	}
	return *row_ids;
}

Vector &ConflictManager::InternalIntermediate() {
	if (!intermediate_vector) {
		intermediate_vector = make_uniq<Vector>(LogicalType::BOOLEAN, true, true, input_size);
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
	if (!SingleIndexTarget()) {
		return;
	}
	if (conflicts.Count() != 0) {
		// We have recorded conflicts from the one index we're interested in
		// We set this so we don't duplicate the conflicts when there are duplicate indexes
		// that also match our conflict target
		single_index_finished = true;
	}
}

void ConflictManager::SetMode(ConflictManagerMode mode) {
	// Only allow SCAN when we have conflict info
	D_ASSERT(mode != ConflictManagerMode::SCAN || conflict_info != nullptr);
	this->mode = mode;
}

void ConflictManager::AddToConflictSet(idx_t chunk_index) {
	if (!conflict_set) {
		conflict_set = make_uniq<unordered_set<idx_t>>();
	}
	auto &set = *conflict_set;
	set.insert(chunk_index);
}

void ConflictManager::AddConflictInternal(idx_t chunk_index, row_t row_id) {
	D_ASSERT(mode == ConflictManagerMode::SCAN);

	// Only when we should not throw on conflict should we get here
	D_ASSERT(!ShouldThrow(chunk_index));
	AddToConflictSet(chunk_index);
	if (SingleIndexTarget()) {
		// If we have identical indexes, only the conflicts of the first index should be recorded
		// as the other index(es) would produce the exact same conflicts anyways
		if (single_index_finished) {
			return;
		}

		// We can be more efficient because we don't need to merge conflicts of multiple indexes
		auto &selection = InternalSelection();
		auto &row_ids = InternalRowIds();
		auto data = FlatVector::GetData<row_t>(row_ids);
		data[selection.Count()] = row_id;
		selection.Append(chunk_index);
	} else {
		auto &intermediate = InternalIntermediate();
		auto data = FlatVector::GetData<bool>(intermediate);
		// Mark this index in the chunk as producing a conflict
		data[chunk_index] = true;
		if (row_id_map.empty()) {
			row_id_map.resize(input_size);
		}
		row_id_map[chunk_index] = row_id;
	}
}

bool ConflictManager::IsConflict(LookupResultType type) {
	switch (type) {
	case LookupResultType::LOOKUP_NULL: {
		if (ShouldIgnoreNulls()) {
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

bool ConflictManager::AddHit(idx_t chunk_index, row_t row_id) {
	D_ASSERT(chunk_index < input_size);
	// First check if this causes a conflict
	if (!IsConflict(LookupResultType::LOOKUP_HIT)) {
		return false;
	}

	// Then check if we should throw on a conflict
	if (ShouldThrow(chunk_index)) {
		return true;
	}
	if (mode == ConflictManagerMode::THROW) {
		// When our mode is THROW, and the chunk index is part of the previously scanned conflicts
		// then we ignore the conflict instead
		D_ASSERT(!ShouldThrow(chunk_index));
		return false;
	}
	D_ASSERT(conflict_info);
	// Because we don't throw, we need to register the conflict
	AddConflictInternal(chunk_index, row_id);
	return false;
}

bool ConflictManager::AddMiss(idx_t chunk_index) {
	D_ASSERT(chunk_index < input_size);
	return IsConflict(LookupResultType::LOOKUP_MISS);
}

bool ConflictManager::AddNull(idx_t chunk_index) {
	D_ASSERT(chunk_index < input_size);
	if (!IsConflict(LookupResultType::LOOKUP_NULL)) {
		return false;
	}
	return AddHit(chunk_index, static_cast<row_t>(DConstants::INVALID_INDEX));
}

bool ConflictManager::SingleIndexTarget() const {
	D_ASSERT(conflict_info);
	// We are only interested in a specific index
	return !conflict_info->column_ids.empty();
}

bool ConflictManager::ShouldThrow(idx_t chunk_index) const {
	if (mode == ConflictManagerMode::SCAN) {
		return false;
	}
	D_ASSERT(mode == ConflictManagerMode::THROW);
	if (conflict_set == nullptr) {
		// No conflicts were scanned, so this conflict is not in the set
		return true;
	}
	auto &set = InternalConflictSet();
	if (set.count(chunk_index)) {
		return false;
	}
	// None of the scanned conflicts arose from this insert tuple
	return true;
}

bool ConflictManager::ShouldIgnoreNulls() const {
	switch (lookup_type) {
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
	if (SingleIndexTarget()) {
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
	for (idx_t i = 0; i < input_size; i++) {
		if (data[i]) {
			selection.Append(i);
		}
	}
	// Now create the row_ids Vector, aligned with the selection vector
	auto &internal_row_ids = InternalRowIds();
	auto row_id_data = FlatVector::GetData<row_t>(internal_row_ids);

	for (idx_t i = 0; i < selection.Count(); i++) {
		D_ASSERT(!row_id_map.empty());
		auto index = selection[i];
		D_ASSERT(index < row_id_map.size());
		auto row_id = row_id_map[index];
		row_id_data[i] = row_id;
	}
	intermediate_vector.reset();
}

VerifyExistenceType ConflictManager::LookupType() const {
	return lookup_type;
}

} // namespace duckdb
