#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/transaction/transaction.hpp"

using namespace duckdb;
using namespace std;

static bool UseVersion(Transaction &transaction, transaction_t id) {
	return id < transaction.start_time || id == transaction.transaction_id;
}

//===--------------------------------------------------------------------===//
// Delete info
//===--------------------------------------------------------------------===//
ChunkDeleteInfo::ChunkDeleteInfo(VersionManager &manager, index_t start_row, ChunkInfoType type)
    : ChunkInfo(manager, start_row, type) {
	for (index_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		deleted[i] = NOT_DELETED_ID;
	}
}

ChunkDeleteInfo::ChunkDeleteInfo(ChunkDeleteInfo &info, ChunkInfoType type)
    : ChunkInfo(info.manager, info.start, type) {
	for (index_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		deleted[i] = info.deleted[i];
	}
}

index_t ChunkDeleteInfo::GetSelVector(Transaction &transaction, sel_t sel_vector[], index_t max_count) {
	index_t count = 0;
	for (index_t i = 0; i < max_count; i++) {
		if (!UseVersion(transaction, deleted[i])) {
			sel_vector[count++] = i;
		}
	}
	return count;
}

bool ChunkDeleteInfo::Fetch(Transaction &transaction, row_t row) {
	return !UseVersion(transaction, deleted[row]);
}

void ChunkDeleteInfo::Delete(Transaction &transaction, row_t rows[], index_t count) {
	// first check the chunk for conflicts
	for (index_t i = 0; i < count; i++) {
		if (deleted[rows[i]] != NOT_DELETED_ID) {
			// tuple was already deleted by another transaction
			throw TransactionException("Conflict on tuple deletion!");
		}
	}
	// after verifying that there are no conflicts we mark the tuples as deleted
	for (index_t i = 0; i < count; i++) {
		deleted[rows[i]] = transaction.transaction_id;
	}
}

void ChunkDeleteInfo::CommitDelete(transaction_t commit_id, row_t rows[], index_t count) {
	for (index_t i = 0; i < count; i++) {
		deleted[rows[i]] = commit_id;
	}
}

//===--------------------------------------------------------------------===//
// Insert info
//===--------------------------------------------------------------------===//
ChunkInsertInfo::ChunkInsertInfo(VersionManager &manager, index_t start_row)
    : ChunkDeleteInfo(manager, start_row, ChunkInfoType::INSERT_INFO) {
	for (index_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		inserted[i] = NOT_DELETED_ID;
	}
}

ChunkInsertInfo::ChunkInsertInfo(ChunkDeleteInfo &info) : ChunkDeleteInfo(info, ChunkInfoType::INSERT_INFO) {
	for (index_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		inserted[i] = NOT_DELETED_ID;
	}
}

index_t ChunkInsertInfo::GetSelVector(Transaction &transaction, sel_t sel_vector[], index_t max_count) {
	index_t count = 0;
	for (index_t i = 0; i < max_count; i++) {
		if (UseVersion(transaction, inserted[i]) && !UseVersion(transaction, deleted[i])) {
			sel_vector[count++] = i;
		}
	}
	return count;
}

bool ChunkInsertInfo::Fetch(Transaction &transaction, row_t row) {
	return UseVersion(transaction, inserted[row]) && !UseVersion(transaction, deleted[row]);
}
