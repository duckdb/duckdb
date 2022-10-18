#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/serializer.hpp"

namespace duckdb {

struct TransactionVersionOperator {
	static bool UseInsertedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return id < start_time || id == transaction_id;
	}

	static bool UseDeletedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return !UseInsertedVersion(start_time, transaction_id, id);
	}
};

struct CommittedVersionOperator {
	static bool UseInsertedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return true;
	}

	static bool UseDeletedVersion(transaction_t min_start_time, transaction_t min_transaction_id, transaction_t id) {
		return (id >= min_start_time && id < TRANSACTION_ID_START) || (id >= min_transaction_id);
	}
};

static bool UseVersion(TransactionData transaction, transaction_t id) {
	return TransactionVersionOperator::UseInsertedVersion(transaction.start_time, transaction.transaction_id, id);
}

unique_ptr<ChunkInfo> ChunkInfo::Deserialize(Deserializer &source) {
	auto type = source.Read<ChunkInfoType>();
	switch (type) {
	case ChunkInfoType::EMPTY_INFO:
		return nullptr;
	case ChunkInfoType::CONSTANT_INFO:
		return ChunkConstantInfo::Deserialize(source);
	case ChunkInfoType::VECTOR_INFO:
		return ChunkVectorInfo::Deserialize(source);
	default:
		throw SerializationException("Could not deserialize Chunk Info Type: unrecognized type");
	}
}

//===--------------------------------------------------------------------===//
// Constant info
//===--------------------------------------------------------------------===//
ChunkConstantInfo::ChunkConstantInfo(idx_t start)
    : ChunkInfo(start, ChunkInfoType::CONSTANT_INFO), insert_id(0), delete_id(NOT_DELETED_ID) {
}

template <class OP>
idx_t ChunkConstantInfo::TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
                                               SelectionVector &sel_vector, idx_t max_count) {
	if (OP::UseInsertedVersion(start_time, transaction_id, insert_id) &&
	    OP::UseDeletedVersion(start_time, transaction_id, delete_id)) {
		return max_count;
	}
	return 0;
}

idx_t ChunkConstantInfo::GetSelVector(TransactionData transaction, SelectionVector &sel_vector, idx_t max_count) {
	return TemplatedGetSelVector<TransactionVersionOperator>(transaction.start_time, transaction.transaction_id,
	                                                         sel_vector, max_count);
}

idx_t ChunkConstantInfo::GetCommittedSelVector(transaction_t min_start_id, transaction_t min_transaction_id,
                                               SelectionVector &sel_vector, idx_t max_count) {
	return TemplatedGetSelVector<CommittedVersionOperator>(min_start_id, min_transaction_id, sel_vector, max_count);
}

bool ChunkConstantInfo::Fetch(TransactionData transaction, row_t row) {
	return UseVersion(transaction, insert_id) && !UseVersion(transaction, delete_id);
}

void ChunkConstantInfo::CommitAppend(transaction_t commit_id, idx_t start, idx_t end) {
	D_ASSERT(start == 0 && end == STANDARD_VECTOR_SIZE);
	insert_id = commit_id;
}

void ChunkConstantInfo::Serialize(Serializer &serializer) {
	// we only need to write this node if any tuple deletions have been committed
	bool is_deleted = insert_id >= TRANSACTION_ID_START || delete_id < TRANSACTION_ID_START;
	if (!is_deleted) {
		serializer.Write<ChunkInfoType>(ChunkInfoType::EMPTY_INFO);
		return;
	}
	serializer.Write<ChunkInfoType>(type);
	serializer.Write<idx_t>(start);
}

unique_ptr<ChunkInfo> ChunkConstantInfo::Deserialize(Deserializer &source) {
	auto start = source.Read<idx_t>();

	auto info = make_unique<ChunkConstantInfo>(start);
	info->insert_id = 0;
	info->delete_id = 0;
	return move(info);
}

//===--------------------------------------------------------------------===//
// Vector info
//===--------------------------------------------------------------------===//
ChunkVectorInfo::ChunkVectorInfo(idx_t start)
    : ChunkInfo(start, ChunkInfoType::VECTOR_INFO), insert_id(0), same_inserted_id(true), any_deleted(false) {
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		inserted[i] = 0;
		deleted[i] = NOT_DELETED_ID;
	}
}

template <class OP>
idx_t ChunkVectorInfo::TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
                                             SelectionVector &sel_vector, idx_t max_count) {
	idx_t count = 0;
	if (same_inserted_id && !any_deleted) {
		// all tuples have the same inserted id: and no tuples were deleted
		if (OP::UseInsertedVersion(start_time, transaction_id, insert_id)) {
			return max_count;
		} else {
			return 0;
		}
	} else if (same_inserted_id) {
		if (!OP::UseInsertedVersion(start_time, transaction_id, insert_id)) {
			return 0;
		}
		// have to check deleted flag
		for (idx_t i = 0; i < max_count; i++) {
			if (OP::UseDeletedVersion(start_time, transaction_id, deleted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	} else if (!any_deleted) {
		// have to check inserted flag
		for (idx_t i = 0; i < max_count; i++) {
			if (OP::UseInsertedVersion(start_time, transaction_id, inserted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	} else {
		// have to check both flags
		for (idx_t i = 0; i < max_count; i++) {
			if (OP::UseInsertedVersion(start_time, transaction_id, inserted[i]) &&
			    OP::UseDeletedVersion(start_time, transaction_id, deleted[i])) {
				sel_vector.set_index(count++, i);
			}
		}
	}
	return count;
}

idx_t ChunkVectorInfo::GetSelVector(transaction_t start_time, transaction_t transaction_id, SelectionVector &sel_vector,
                                    idx_t max_count) {
	return TemplatedGetSelVector<TransactionVersionOperator>(start_time, transaction_id, sel_vector, max_count);
}

idx_t ChunkVectorInfo::GetCommittedSelVector(transaction_t min_start_id, transaction_t min_transaction_id,
                                             SelectionVector &sel_vector, idx_t max_count) {
	return TemplatedGetSelVector<CommittedVersionOperator>(min_start_id, min_transaction_id, sel_vector, max_count);
}

idx_t ChunkVectorInfo::GetSelVector(TransactionData transaction, SelectionVector &sel_vector, idx_t max_count) {
	return GetSelVector(transaction.start_time, transaction.transaction_id, sel_vector, max_count);
}

bool ChunkVectorInfo::Fetch(TransactionData transaction, row_t row) {
	return UseVersion(transaction, inserted[row]) && !UseVersion(transaction, deleted[row]);
}

idx_t ChunkVectorInfo::Delete(transaction_t transaction_id, row_t rows[], idx_t count) {
	any_deleted = true;

	idx_t deleted_tuples = 0;
	for (idx_t i = 0; i < count; i++) {
		if (deleted[rows[i]] == transaction_id) {
			continue;
		}
		// first check the chunk for conflicts
		if (deleted[rows[i]] != NOT_DELETED_ID) {
			// tuple was already deleted by another transaction
			throw TransactionException("Conflict on tuple deletion!");
		}
		// after verifying that there are no conflicts we mark the tuple as deleted
		deleted[rows[i]] = transaction_id;
		rows[deleted_tuples] = rows[i];
		deleted_tuples++;
	}
	return deleted_tuples;
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

void ChunkVectorInfo::CommitAppend(transaction_t commit_id, idx_t start, idx_t end) {
	if (same_inserted_id) {
		insert_id = commit_id;
	}
	for (idx_t i = start; i < end; i++) {
		inserted[i] = commit_id;
	}
}

void ChunkVectorInfo::Serialize(Serializer &serializer) {
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	transaction_t start_time = TRANSACTION_ID_START - 1;
	transaction_t transaction_id = DConstants::INVALID_INDEX;
	idx_t count = GetSelVector(start_time, transaction_id, sel, STANDARD_VECTOR_SIZE);
	if (count == STANDARD_VECTOR_SIZE) {
		// nothing is deleted: skip writing anything
		serializer.Write<ChunkInfoType>(ChunkInfoType::EMPTY_INFO);
		return;
	}
	if (count == 0) {
		// everything is deleted: write a constant vector
		serializer.Write<ChunkInfoType>(ChunkInfoType::CONSTANT_INFO);
		serializer.Write<idx_t>(start);
		return;
	}
	// write a boolean vector
	serializer.Write<ChunkInfoType>(ChunkInfoType::VECTOR_INFO);
	serializer.Write<idx_t>(start);
	bool deleted_tuples[STANDARD_VECTOR_SIZE];
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		deleted_tuples[i] = true;
	}
	for (idx_t i = 0; i < count; i++) {
		deleted_tuples[sel.get_index(i)] = false;
	}
	serializer.WriteData((data_ptr_t)deleted_tuples, sizeof(bool) * STANDARD_VECTOR_SIZE);
}

unique_ptr<ChunkInfo> ChunkVectorInfo::Deserialize(Deserializer &source) {
	auto start = source.Read<idx_t>();

	auto result = make_unique<ChunkVectorInfo>(start);
	result->any_deleted = true;
	bool deleted_tuples[STANDARD_VECTOR_SIZE];
	source.ReadData((data_ptr_t)deleted_tuples, sizeof(bool) * STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (deleted_tuples[i]) {
			result->deleted[i] = 0;
		}
	}
	return move(result);
}

} // namespace duckdb
