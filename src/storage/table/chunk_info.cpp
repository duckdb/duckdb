#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {
using namespace std;

static bool UseVersion(Transaction &transaction, transaction_t id) {
	return id < transaction.start_time || id == transaction.transaction_id;
}

//===--------------------------------------------------------------------===//
// Constant info
//===--------------------------------------------------------------------===//
ChunkConstantInfo::ChunkConstantInfo(MorselInfo &morsel) :
    ChunkInfo(morsel, ChunkInfoType::CONSTANT_INFO), insert_id(0), delete_id(NOT_DELETED_ID) {
}


idx_t ChunkConstantInfo::GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) {
	if (UseVersion(transaction, insert_id) && !UseVersion(transaction, delete_id)) {
		return max_count;
	}
	return 0;
}

bool ChunkConstantInfo::Fetch(Transaction &transaction, row_t row) {
	return UseVersion(transaction, insert_id) && !UseVersion(transaction, delete_id);
}

//===--------------------------------------------------------------------===//
// Vector info
//===--------------------------------------------------------------------===//
ChunkVectorInfo::ChunkVectorInfo(MorselInfo &morsel) :
	ChunkInfo(morsel, ChunkInfoType::VECTOR_INFO), insert_id(0),
	same_inserted_id(true), any_deleted(false) {
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		inserted[i] = 0;
		deleted[i] = NOT_DELETED_ID;
	}
}

idx_t ChunkVectorInfo::GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) {
	idx_t count = 0;
	if (same_inserted_id && !any_deleted) {
		// all tuples have the same inserted id: and no tuples were deleted
		if (UseVersion(transaction, insert_id)) {
			return max_count;
		} else {
			return 0;
		}
	} else if (same_inserted_id) {
		if (!UseVersion(transaction, insert_id)) {
			return 0;
		}
		// have to check deleted flag
		for (idx_t i = 0; i < max_count; i++) {
			if (!UseVersion(transaction, deleted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	} else if (!any_deleted) {
		// have to check inserted flag
		for (idx_t i = 0; i < max_count; i++) {
			if (UseVersion(transaction, inserted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	} else {
		// have to check both flags
		for (idx_t i = 0; i < max_count; i++) {
			if (UseVersion(transaction, inserted[i]) && !UseVersion(transaction, deleted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	}
	return count;
}

bool ChunkVectorInfo::Fetch(Transaction &transaction, row_t row) {
	return UseVersion(transaction, inserted[row]);
}

void ChunkVectorInfo::Delete(Transaction &transaction, row_t rows[], idx_t count) {
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

void ChunkVectorInfo::CommitDelete(transaction_t commit_id, row_t rows[], idx_t count) {
	for (idx_t i = 0; i < count; i++) {
		deleted[rows[i]] = commit_id;
	}
}

void ChunkVectorInfo::Append(idx_t start, idx_t end, transaction_t commit_id) {
	if (start == 0) {
		insert_id = commit_id;
	} else if (insert_id != commit_id) {
		same_inserted_id = false;
		insert_id = NOT_DELETED_ID;
	}
	for (idx_t i = start; i < end; i++) {
		inserted[i] = commit_id;
	}
}

} // namespace duckdb
