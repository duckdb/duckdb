#include "duckdb/storage/table/chunk_info.hpp"

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/transaction/delete_info.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"

namespace duckdb {

struct StandardInsertOperator {
	static bool UseInsertedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return id < start_time || id == transaction_id;
	}
};

struct IncludeAllInsertedOperator {
	static bool UseInsertedVersion(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return true;
	}
};

struct StandardDeleteOperator {
	static bool IsDeleted(transaction_t start_time, transaction_t transaction_id, transaction_t id) {
		return StandardInsertOperator::UseInsertedVersion(start_time, transaction_id, id);
	}
};

struct CommittedDeleteOperator {
	static bool IsDeleted(transaction_t min_start_time, transaction_t min_transaction_id, transaction_t id) {
		// check if this row was deleted before the given start time
		return id < min_start_time;
	}
};

struct IncludeAllDeletedOperator {
	static bool IsDeleted(transaction_t min_start_time, transaction_t min_transaction_id, transaction_t id) {
		return false;
	}
};

static bool UseVersion(TransactionData transaction, transaction_t id) {
	return StandardInsertOperator::UseInsertedVersion(transaction.start_time, transaction.transaction_id, id);
}

bool ChunkInfo::Cleanup(transaction_t lowest_transaction) const {
	return false;
}

void ChunkInfo::Write(WriteStream &writer, transaction_t checkpoint_id) const {
	writer.Write<ChunkInfoType>(type);
}

unique_ptr<ChunkInfo> ChunkInfo::Read(FixedSizeAllocator &allocator, ReadStream &reader) {
	auto type = reader.Read<ChunkInfoType>();
	switch (type) {
	case ChunkInfoType::EMPTY_INFO:
		return nullptr;
	case ChunkInfoType::CONSTANT_INFO:
		return ChunkConstantInfo::Read(reader);
	case ChunkInfoType::VECTOR_INFO:
		return ChunkVectorInfo::Read(allocator, reader);
	default:
		throw SerializationException("Could not deserialize Chunk Info Type: unrecognized type");
	}
}

idx_t ChunkInfo::GetCommittedDeletedCount(idx_t max_count) const {
	ScanOptions options(TransactionData(0, TRANSACTION_ID_START));
	options.insert_type = InsertedScanType::ALL_ROWS;
	options.delete_type = DeletedScanType::OMIT_COMMITTED_DELETES;
	idx_t not_deleted_count = GetSelVector(options, nullptr, max_count);
	return max_count - not_deleted_count;
}

idx_t ChunkInfo::GetCheckpointRowCount(TransactionData transaction, idx_t max_count) {
	ScanOptions options(transaction);
	options.delete_type = DeletedScanType::INCLUDE_ALL_DELETED;
	return GetSelVector(options, nullptr, max_count);
}

//===--------------------------------------------------------------------===//
// Constant info
//===--------------------------------------------------------------------===//
ChunkConstantInfo::ChunkConstantInfo(idx_t start)
    : ChunkInfo(start, ChunkInfoType::CONSTANT_INFO), insert_id(0), delete_id(NOT_DELETED_ID) {
}

template <class INSERT_OP, class DELETE_OP>
idx_t ChunkConstantInfo::TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
                                               idx_t max_count) const {
	if (INSERT_OP::UseInsertedVersion(start_time, transaction_id, insert_id) &&
	    !DELETE_OP::IsDeleted(start_time, transaction_id, delete_id)) {
		return max_count;
	}
	return 0;
}

idx_t ChunkConstantInfo::GetSelVector(ScanOptions options, optional_ptr<SelectionVector> sel_vector,
                                      idx_t max_count) const {
	auto &transaction = options.transaction;
	if (options.insert_type == InsertedScanType::STANDARD) {
		if (!StandardInsertOperator::UseInsertedVersion(transaction.start_time, transaction.transaction_id,
		                                                insert_id)) {
			return 0;
		}
	}
	if (options.delete_type == DeletedScanType::STANDARD) {
		if (StandardDeleteOperator::IsDeleted(transaction.start_time, transaction.transaction_id, delete_id)) {
			return 0;
		}
	} else if (options.delete_type == DeletedScanType::OMIT_COMMITTED_DELETES) {
		if (CommittedDeleteOperator::IsDeleted(transaction.start_time, transaction.transaction_id, delete_id)) {
			return 0;
		}
	}
	return max_count;
}

bool ChunkConstantInfo::Fetch(TransactionData transaction, row_t row) {
	return UseVersion(transaction, insert_id) && !UseVersion(transaction, delete_id);
}

void ChunkConstantInfo::CommitAppend(transaction_t commit_id, idx_t start, idx_t end) {
	D_ASSERT(start == 0 && end == STANDARD_VECTOR_SIZE);
	insert_id = commit_id;
}

bool ChunkConstantInfo::HasDeletes(transaction_t transaction_id) const {
	if (transaction_id == MAX_TRANSACTION_ID) {
		transaction_id = TRANSACTION_ID_START - 1;
	}
	bool is_deleted = insert_id >= TRANSACTION_ID_START || delete_id <= transaction_id;
	return is_deleted;
}

bool ChunkConstantInfo::Cleanup(transaction_t lowest_transaction) const {
	if (delete_id != NOT_DELETED_ID) {
		// the chunk info is labeled as deleted - we need to keep it around
		return false;
	}
	if (insert_id > lowest_transaction) {
		// there are still transactions active that need this ChunkInfo
		return false;
	}
	return true;
}

void ChunkConstantInfo::Write(WriteStream &writer, transaction_t checkpoint_id) const {
	D_ASSERT(HasDeletes(checkpoint_id));
	ChunkInfo::Write(writer, checkpoint_id);
	writer.Write<idx_t>(start);
}

unique_ptr<ChunkInfo> ChunkConstantInfo::Read(ReadStream &reader) {
	auto start = reader.Read<idx_t>();
	auto info = make_uniq<ChunkConstantInfo>(start);
	info->insert_id = 0;
	info->delete_id = 0;
	return std::move(info);
}

string ChunkConstantInfo::ToString(idx_t max_count) const {
	string result;
	result += "Constant [Count: " + to_string(max_count);
	result += ", ";
	result += "Insert Id: " + to_string(insert_id);
	if (delete_id != NOT_DELETED_ID) {
		result += ", Delete Id: " + to_string(delete_id);
	}
	result += "]";
	return result;
}

//===--------------------------------------------------------------------===//
// Vector info
//===--------------------------------------------------------------------===//
ChunkVectorInfo::ChunkVectorInfo(FixedSizeAllocator &allocator_p, idx_t start, transaction_t insert_id_p)
    : ChunkInfo(start, ChunkInfoType::VECTOR_INFO), allocator(allocator_p), constant_insert_id(insert_id_p) {
}

ChunkVectorInfo::~ChunkVectorInfo() {
	if (AnyDeleted()) {
		allocator.Free(deleted_data);
	}
	if (!HasConstantInsertionId()) {
		allocator.Free(inserted_data);
	}
}

template <class INSERT_OP, class DELETE_OP>
idx_t ChunkVectorInfo::TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
                                             optional_ptr<SelectionVector> sel_vector, idx_t max_count) const {
	if (HasConstantInsertionId()) {
		if (!AnyDeleted()) {
			// all tuples have the same inserted id: and no tuples were deleted
			if (INSERT_OP::UseInsertedVersion(start_time, transaction_id, ConstantInsertId())) {
				return max_count;
			} else {
				return 0;
			}
		}
		if (!INSERT_OP::UseInsertedVersion(start_time, transaction_id, ConstantInsertId())) {
			return 0;
		}
		// have to check deleted flag
		idx_t count = 0;
		auto segment = allocator.GetHandle(GetDeletedPointer());
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < max_count; i++) {
			if (DELETE_OP::IsDeleted(start_time, transaction_id, deleted[i])) {
				continue;
			}
			if (sel_vector) {
				sel_vector->set_index(count, i);
			}
			count++;
		}
		return count;
	}
	if (!AnyDeleted()) {
		// have to check inserted flag
		auto insert_segment = allocator.GetHandle(GetInsertedPointer());
		auto inserted = insert_segment.GetPtr<transaction_t>();

		idx_t count = 0;
		for (idx_t i = 0; i < max_count; i++) {
			if (!INSERT_OP::UseInsertedVersion(start_time, transaction_id, inserted[i])) {
				continue;
			}
			if (sel_vector) {
				sel_vector->set_index(count, i);
			}
			count++;
		}
		return count;
	}

	idx_t count = 0;
	// have to check both flags
	auto insert_segment = allocator.GetHandle(GetInsertedPointer());
	auto inserted = insert_segment.GetPtr<transaction_t>();

	auto delete_segment = allocator.GetHandle(GetDeletedPointer());
	auto deleted = delete_segment.GetPtr<transaction_t>();
	for (idx_t i = 0; i < max_count; i++) {
		if (!INSERT_OP::UseInsertedVersion(start_time, transaction_id, inserted[i])) {
			continue;
		}
		if (DELETE_OP::IsDeleted(start_time, transaction_id, deleted[i])) {
			continue;
		}
		if (sel_vector) {
			sel_vector->set_index(count, i);
		}
		count++;
	}
	return count;
}

idx_t ChunkVectorInfo::GetSelVector(ScanOptions options, optional_ptr<SelectionVector> sel_vector,
                                    idx_t max_count) const {
	auto &transaction = options.transaction;
	if (options.insert_type == InsertedScanType::STANDARD) {
		if (options.delete_type == DeletedScanType::STANDARD) {
			return TemplatedGetSelVector<StandardInsertOperator, StandardDeleteOperator>(
			    transaction.start_time, transaction.transaction_id, sel_vector, max_count);
		}
		if (options.delete_type == DeletedScanType::INCLUDE_ALL_DELETED) {
			return TemplatedGetSelVector<StandardInsertOperator, IncludeAllDeletedOperator>(
			    transaction.start_time, transaction.transaction_id, sel_vector, max_count);
		}
		if (options.delete_type == DeletedScanType::OMIT_COMMITTED_DELETES) {
			return TemplatedGetSelVector<StandardInsertOperator, CommittedDeleteOperator>(
			    transaction.start_time, transaction.transaction_id, sel_vector, max_count);
		}
	}
	if (options.insert_type == InsertedScanType::ALL_ROWS) {
		if (options.delete_type == DeletedScanType::STANDARD) {
			return TemplatedGetSelVector<IncludeAllInsertedOperator, StandardDeleteOperator>(
			    transaction.start_time, transaction.transaction_id, sel_vector, max_count);
		}
		if (options.delete_type == DeletedScanType::OMIT_COMMITTED_DELETES) {
			return TemplatedGetSelVector<IncludeAllInsertedOperator, CommittedDeleteOperator>(
			    transaction.start_time, transaction.transaction_id, sel_vector, max_count);
		}
	}
	throw InternalException("Unsupported combination of insert / delete types in ChunkVectorInfo::GetSelVector");
}

bool ChunkVectorInfo::Fetch(TransactionData transaction, row_t row) {
	transaction_t fetch_insert_id;
	transaction_t fetch_deleted_id;
	if (HasConstantInsertionId()) {
		fetch_insert_id = ConstantInsertId();
	} else {
		auto insert_segment = allocator.GetHandle(GetInsertedPointer());
		auto inserted = insert_segment.GetPtr<transaction_t>();
		fetch_insert_id = inserted[row];
	}
	if (!AnyDeleted()) {
		fetch_deleted_id = NOT_DELETED_ID;
	} else {
		auto delete_segment = allocator.GetHandle(GetDeletedPointer());
		auto deleted = delete_segment.GetPtr<transaction_t>();
		fetch_deleted_id = deleted[row];
	}

	return UseVersion(transaction, fetch_insert_id) && !UseVersion(transaction, fetch_deleted_id);
}

IndexPointer ChunkVectorInfo::GetInsertedPointer() const {
	if (HasConstantInsertionId()) {
		throw InternalException("ChunkVectorInfo: insert id requested but insertions were not initialized");
	}
	return inserted_data;
}

IndexPointer ChunkVectorInfo::GetDeletedPointer() const {
	if (!AnyDeleted()) {
		throw InternalException("ChunkVectorInfo: deleted id requested but deletions were not initialized");
	}
	return deleted_data;
}

IndexPointer ChunkVectorInfo::GetInitializedInsertedPointer() {
	if (HasConstantInsertionId()) {
		transaction_t constant_id = ConstantInsertId();

		inserted_data = allocator.New();
		inserted_data.SetMetadata(1);
		auto segment = allocator.GetHandle(inserted_data);
		auto inserted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			inserted[i] = constant_id;
		}
	}
	return inserted_data;
}

IndexPointer ChunkVectorInfo::GetInitializedDeletedPointer() {
	if (!AnyDeleted()) {
		deleted_data = allocator.New();
		deleted_data.SetMetadata(1);
		auto segment = allocator.GetHandle(deleted_data);
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			deleted[i] = NOT_DELETED_ID;
		}
	}
	return deleted_data;
}

idx_t ChunkVectorInfo::Delete(transaction_t transaction_id, row_t rows[], idx_t count) {
	auto segment = allocator.GetHandle(GetInitializedDeletedPointer());
	auto deleted = segment.GetPtr<transaction_t>();

	idx_t deleted_tuples = 0;
	for (idx_t i = 0; i < count; i++) {
		if (deleted[rows[i]] == transaction_id) {
			continue;
		}
		// first check the chunk for conflicts
		if (deleted[rows[i]] != NOT_DELETED_ID) {
			// tuple was already deleted by another transaction - conflict
			// unset any deleted tuples we set in this loop
			for (idx_t k = 0; k < i; k++) {
				deleted[rows[k]] = NOT_DELETED_ID;
			}
			throw TransactionException("Conflict on tuple deletion!");
		}
		// after verifying that there are no conflicts we mark the tuple as deleted
		deleted[rows[i]] = transaction_id;
		rows[deleted_tuples] = rows[i];
		deleted_tuples++;
	}
	return deleted_tuples;
}

void ChunkVectorInfo::CommitDelete(transaction_t commit_id, const DeleteInfo &info) {
	auto segment = allocator.GetHandle(GetDeletedPointer());
	auto deleted = segment.GetPtr<transaction_t>();

	if (info.is_consecutive) {
		for (idx_t i = 0; i < info.count; i++) {
			deleted[i] = commit_id;
		}
	} else {
		auto rows = info.GetRows();
		for (idx_t i = 0; i < info.count; i++) {
			deleted[rows[i]] = commit_id;
		}
	}
}

void ChunkVectorInfo::Append(idx_t start, idx_t end, transaction_t commit_id) {
	if (start == 0) {
		// first insert to this vector - just assign the commit id
		constant_insert_id = commit_id;
		return;
	}
	if (HasConstantInsertionId() && ConstantInsertId() == commit_id) {
		// we are inserting again, but we have the same id as before - still the same insert id
		return;
	}

	auto segment = allocator.GetHandle(GetInitializedInsertedPointer());
	auto inserted = segment.GetPtr<transaction_t>();
	for (idx_t i = start; i < end; i++) {
		inserted[i] = commit_id;
	}
}

void ChunkVectorInfo::CommitAppend(transaction_t commit_id, idx_t start, idx_t end) {
	if (HasConstantInsertionId()) {
		constant_insert_id = commit_id;
		return;
	}
	auto segment = allocator.GetHandle(GetInsertedPointer());
	auto inserted = segment.GetPtr<transaction_t>();

	for (idx_t i = start; i < end; i++) {
		inserted[i] = commit_id;
	}
}

bool ChunkVectorInfo::Cleanup(transaction_t lowest_transaction) const {
	if (AnyDeleted()) {
		// if any rows are deleted we can't clean-up
		return false;
	}
	// check if the insertion markers have to be used by all transactions going forward
	if (!HasConstantInsertionId()) {
		auto segment = allocator.GetHandle(GetInsertedPointer());
		auto inserted = segment.GetPtr<transaction_t>();

		for (idx_t idx = 1; idx < STANDARD_VECTOR_SIZE; idx++) {
			if (inserted[idx] > lowest_transaction) {
				// transaction was inserted after the lowest transaction start
				// we still need to use an older version - cannot compress
				return false;
			}
		}
	} else if (ConstantInsertId() > lowest_transaction) {
		// transaction was inserted after the lowest transaction start
		// we still need to use an older version - cannot compress
		return false;
	}
	return true;
}

bool ChunkVectorInfo::HasDeletes(transaction_t transaction_id) const {
	if (!AnyDeleted()) {
		return false;
	}
	if (transaction_id == MAX_TRANSACTION_ID) {
		return true;
	}
	auto segment = allocator.GetHandle(deleted_data);
	auto deleted = segment.GetPtr<transaction_t>();

	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (deleted[i] <= transaction_id) {
			return true;
		}
	}
	return false;
}

bool ChunkVectorInfo::AnyDeleted() const {
	return deleted_data.HasMetadata();
}

bool ChunkVectorInfo::HasConstantInsertionId() const {
	return !inserted_data.HasMetadata();
}

string ChunkVectorInfo::ToString(idx_t max_count) const {
	string result;
	result += "Vector [Count: " + to_string(max_count);
	result += ", ";
	if (HasConstantInsertionId()) {
		result += "Insert Id: " + to_string(constant_insert_id);
	} else {
		result += "Insert Ids: [";
		auto segment = allocator.GetHandle(GetInsertedPointer());
		auto inserted = segment.GetPtr<transaction_t>();

		for (idx_t idx = 0; idx < max_count; idx++) {
			if (idx > 0) {
				result += ", ";
			}
			result += to_string(inserted[idx]);
		}
		result += "]";
	}
	result += "]";
	return result;
}

transaction_t ChunkVectorInfo::ConstantInsertId() const {
	if (!HasConstantInsertionId()) {
		throw InternalException("ConstantInsertId() called but vector info does not have a constant insertion id");
	}
	return constant_insert_id;
}

void ChunkVectorInfo::Write(WriteStream &writer, transaction_t checkpoint_id) const {
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	transaction_t start_time = checkpoint_id == MAX_TRANSACTION_ID ? TRANSACTION_ID_START - 1 : checkpoint_id + 1;
	transaction_t transaction_id = DConstants::INVALID_INDEX;
	idx_t count = GetSelVector(TransactionData(transaction_id, start_time), sel, STANDARD_VECTOR_SIZE);
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
	ChunkInfo::Write(writer, checkpoint_id);
	writer.Write<idx_t>(start);
	ValidityMask mask(STANDARD_VECTOR_SIZE);
	mask.Initialize(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		mask.SetInvalid(sel.get_index(i));
	}
	mask.Write(writer, STANDARD_VECTOR_SIZE);
}

unique_ptr<ChunkInfo> ChunkVectorInfo::Read(FixedSizeAllocator &allocator, ReadStream &reader) {
	auto start = reader.Read<idx_t>();
	auto result = make_uniq<ChunkVectorInfo>(allocator, start);
	ValidityMask mask;
	mask.Read(reader, STANDARD_VECTOR_SIZE);

	auto segment = allocator.GetHandle(result->GetInitializedDeletedPointer());
	auto deleted = segment.GetPtr<transaction_t>();
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (mask.RowIsValid(i)) {
			deleted[i] = 0;
		}
	}
	return std::move(result);
}

} // namespace duckdb
