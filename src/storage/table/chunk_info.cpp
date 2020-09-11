#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
using namespace std;

static bool UseVersion(Transaction &transaction, transaction_t id) {
	return id < transaction.start_time || id == transaction.transaction_id;
}

//===--------------------------------------------------------------------===//
// Delete info
//===--------------------------------------------------------------------===//
ChunkDeleteInfo::ChunkDeleteInfo(VersionManager &manager, idx_t start_row, ChunkInfoType type)
    : ChunkInfo(manager, start_row, type), any_deleted(false) {
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		deleted[i] = NOT_DELETED_ID;
	}
}

ChunkDeleteInfo::ChunkDeleteInfo(ChunkDeleteInfo &info, ChunkInfoType type)
    : ChunkInfo(info.manager, info.start, type), any_deleted(false) {
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		deleted[i] = info.deleted[i];
	}
}

idx_t ChunkDeleteInfo::GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) {
	if (!any_deleted) {
		return max_count;
	}
	idx_t count = 0;
	for (idx_t i = 0; i < max_count; i++) {
		if (!UseVersion(transaction, deleted[i])) {
			sel_vector.set_index(count++, i);
		}
	}
	return count;
}

bool ChunkDeleteInfo::Fetch(Transaction &transaction, row_t row) {
	return !UseVersion(transaction, deleted[row]);
}

void ChunkDeleteInfo::Delete(Transaction &transaction, row_t rows[], idx_t count) {
	any_deleted = true;

	// first check the chunk for conflicts
	for (idx_t i = 0; i < count; i++) {
		if (deleted[rows[i]] != NOT_DELETED_ID) {
			// tuple was already deleted by another transaction
			throw TransactionException("Conflict on tuple deletion!");
		}
	}
	// after verifying that there are no conflicts we mark the tuples as deleted
	for (idx_t i = 0; i < count; i++) {
		deleted[rows[i]] = transaction.transaction_id;
	}
}

void ChunkDeleteInfo::CommitDelete(transaction_t commit_id, row_t rows[], idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		deleted[rows[i]] = commit_id;
	}
}

//===--------------------------------------------------------------------===//
// Insert info
//===--------------------------------------------------------------------===//
ChunkInsertInfo::ChunkInsertInfo(VersionManager &manager, idx_t start_row)
    : ChunkDeleteInfo(manager, start_row, ChunkInfoType::INSERT_INFO), all_same_id(true) {
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		inserted[i] = NOT_DELETED_ID;
	}
}

ChunkInsertInfo::ChunkInsertInfo(ChunkDeleteInfo &info)
    : ChunkDeleteInfo(info, ChunkInfoType::INSERT_INFO), all_same_id(true) {
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		inserted[i] = NOT_DELETED_ID;
	}
}

idx_t ChunkInsertInfo::GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) {
	if (all_same_id && !any_deleted) {
		// all tuples have the same id, and nothing is deleted: only need to check same id
		if (UseVersion(transaction, same_id)) {
			return max_count;
		} else {
			return 0;
		}
	} else if (all_same_id) {
		// all same id, but elements are deleted
		// first check the insertion flag to see if we need to use any elements at all
		if (!UseVersion(transaction, same_id)) {
			return 0;
		}
		// have to check the deleted count
		return ChunkDeleteInfo::GetSelVector(transaction, sel_vector, max_count);
	} else if (!any_deleted) {
		// not same id, but nothing is deleted
		// only check insertion flag
		idx_t count = 0;
		for (idx_t i = 0; i < max_count; i++) {
			if (UseVersion(transaction, inserted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
		return count;
	} else {
		// not same id, and elements are deleted
		// have to check both flags
		idx_t count = 0;
		for (idx_t i = 0; i < max_count; i++) {
			if (UseVersion(transaction, inserted[i]) && !UseVersion(transaction, deleted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
		return count;
	}
}

bool ChunkInsertInfo::Fetch(Transaction &transaction, row_t row) {
	return UseVersion(transaction, inserted[row]) && !UseVersion(transaction, deleted[row]);
}

void ChunkInsertInfo::Append(idx_t start, idx_t end, transaction_t commit_id) {
	if (start == 0) {
		same_id = commit_id;
	} else if (same_id != commit_id) {
		all_same_id = false;
		same_id = NOT_DELETED_ID;
	}
	for (idx_t i = start; i < end; i++) {
		inserted[i] = commit_id;
	}
}

} // namespace duckdb
