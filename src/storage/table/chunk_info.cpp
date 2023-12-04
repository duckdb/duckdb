#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/serializer/serializer.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"

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

void ChunkInfo::Write(WriteStream &writer) const {
	writer.Write<ChunkInfoType>(type);
}

unique_ptr<ChunkInfo> ChunkInfo::Read(ReadStream &reader) {
	auto type = reader.Read<ChunkInfoType>();
	switch (type) {
	case ChunkInfoType::EMPTY_INFO:
		return nullptr;
	case ChunkInfoType::CONSTANT_INFO:
		return ChunkConstantInfo::Read(reader);
	case ChunkInfoType::VECTOR_INFO:
		return ChunkVectorInfo::Read(reader);
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
                                               SelectionVector &sel_vector, idx_t max_count) const {
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

bool ChunkConstantInfo::HasDeletes() const {
	bool is_deleted = insert_id >= TRANSACTION_ID_START || delete_id < TRANSACTION_ID_START;
	return is_deleted;
}

idx_t ChunkConstantInfo::GetCommittedDeletedCount(idx_t max_count) {
	return delete_id < TRANSACTION_ID_START ? max_count : 0;
}

void ChunkConstantInfo::Write(WriteStream &writer) const {
	D_ASSERT(HasDeletes());
	ChunkInfo::Write(writer);
	writer.Write<idx_t>(start);
}

unique_ptr<ChunkInfo> ChunkConstantInfo::Read(ReadStream &reader) {
	auto start = reader.Read<idx_t>();
	auto info = make_uniq<ChunkConstantInfo>(start);
	info->insert_id = 0;
	info->delete_id = 0;
	return std::move(info);
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
                                             SelectionVector &sel_vector, idx_t max_count) const {
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
                                    idx_t max_count) const {
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

bool ChunkVectorInfo::HasDeletes() const {
	return any_deleted;
}

idx_t ChunkVectorInfo::GetCommittedDeletedCount(idx_t max_count) {
	if (!any_deleted) {
		return 0;
	}
	idx_t delete_count = 0;
	for (idx_t i = 0; i < max_count; i++) {
		if (deleted[i] < TRANSACTION_ID_START) {
			delete_count++;
		}
	}
	return delete_count;
}

void ChunkVectorInfo::Write(WriteStream &writer) const {
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	transaction_t start_time = TRANSACTION_ID_START - 1;
	transaction_t transaction_id = DConstants::INVALID_INDEX;
	idx_t count = GetSelVector(start_time, transaction_id, sel, STANDARD_VECTOR_SIZE);
	if (count == STANDARD_VECTOR_SIZE) {
		// nothing is deleted: skip writing anything
		writer.Write<ChunkInfoType>(ChunkInfoType::EMPTY_INFO);
		return;
	}
	if (count == 0) {
		// everything is deleted: write a constant vector
		writer.Write<ChunkInfoType>(ChunkInfoType::CONSTANT_INFO);
		writer.Write<idx_t>(start);
		return;
	}
	// write a boolean vector
	ChunkInfo::Write(writer);
	writer.Write<idx_t>(start);
	ValidityMask mask(STANDARD_VECTOR_SIZE);
	mask.Initialize(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		mask.SetInvalid(sel.get_index(i));
	}
	mask.Write(writer, STANDARD_VECTOR_SIZE);
}

unique_ptr<ChunkInfo> ChunkVectorInfo::Read(ReadStream &reader) {
	auto start = reader.Read<idx_t>();
	auto result = make_uniq<ChunkVectorInfo>(start);
	result->any_deleted = true;
	ValidityMask mask;
	mask.Read(reader, STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (mask.RowIsValid(i)) {
			result->deleted[i] = 0;
		}
	}
	return std::move(result);
}

} // namespace duckdb
