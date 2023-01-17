#include "duckdb/common/types/conflict_manager.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/execution/index/art/art.hpp"
#include "duckdb/common/types/constraint_conflict_info.hpp"

namespace duckdb {

ConflictManager::ConflictManager(VerifyExistenceType lookup_type, idx_t input_size, ConflictInfo *conflict_info)
    : lookup_type(lookup_type), input_size(input_size), conflict_info(conflict_info) {
}

ManagedSelection &ConflictManager::InternalSelection() {
	if (!conflicts.Initialized()) {
		conflicts.Initialize(input_size);
	}
	return conflicts;
}

Vector &ConflictManager::InternalRowIds() {
	if (!row_ids) {
		row_ids = make_unique<Vector>(LogicalType::ROW_TYPE, input_size);
	}
	return *row_ids;
}

Vector &ConflictManager::InternalIntermediate() {
	if (!intermediate_vector) {
		intermediate_vector = make_unique<Vector>(LogicalType::BOOLEAN, true, true, input_size);
	}
	return *intermediate_vector;
}

const ConflictInfo &ConflictManager::GetConflictInfo() const {
	D_ASSERT(conflict_info);
	return *conflict_info;
}

void ConflictManager::FinishLookup() {
	if (ShouldThrowOnConflict()) {
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

void ConflictManager::AddOrUpdateConflict(row_t row_id, Index &index, bool matches_target) {
	D_ASSERT(conflict_info);
	if (!conflict_map) {
		conflict_map = make_unique<unordered_map<row_t, bool>>();
	}
	auto &map = *conflict_map;
	auto entry = map.find(row_id);
	if (entry == map.end()) {
		// No conflict on this row_id has been found yet
		map[row_id] = matches_target;
		return;
	}
	if (entry->second == true) {
		// Conflict already part of the conflict target, no need to update anything
		return;
	}
	entry->second = matches_target;
}

void ConflictManager::AddConflictInternal(idx_t chunk_index, row_t row_id, Index &index) {
	// Only when we should not throw on conflict should we get here
	D_ASSERT(!ShouldThrowOnConflict());
	bool part_of_conflict_target = conflict_info->ConflictTargetMatches(index);
	AddOrUpdateConflict(row_id, index, part_of_conflict_target);
	if (!part_of_conflict_target) {
		return;
	}
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

bool ConflictManager::AddHit(idx_t chunk_index, row_t row_id, Index &index) {
	D_ASSERT(chunk_index < input_size);
	if (ShouldThrowOnConflict()) {
		return true;
	}
	AddConflictInternal(chunk_index, row_id, index);
	return false;
}

bool ConflictManager::AddMiss(idx_t chunk_index, Index &index) {
	D_ASSERT(chunk_index < input_size);
	return false;
	// FIXME: If we record a miss as a conflict when the verify type is APPEND_FK, then we can simplify the checks in
	// VerifyForeignKeyConstraint This also means we should not record a hit as a conflict when the verify type is
	// APPEND_FK
}

bool ConflictManager::AddNull(idx_t chunk_index, Index &index) {
	D_ASSERT(chunk_index < input_size);
	if (ShouldIgnoreNulls()) {
		return false;
	}
	return AddHit(chunk_index, DConstants::INVALID_INDEX, index);
}

bool ConflictManager::SingleIndexTarget() const {
	D_ASSERT(conflict_info);
	// We are only interested in a specific index
	return !conflict_info->column_ids.empty();
}

bool ConflictManager::ShouldThrowOnConflict() const {
	return conflict_info == nullptr;
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
	auto &row_ids = InternalRowIds();
	auto row_id_data = FlatVector::GetData<row_t>(row_ids);

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
	return this->lookup_type;
}

void ConflictManager::SetIndexCount(idx_t count) {
	index_count = count;
}

} // namespace duckdb
