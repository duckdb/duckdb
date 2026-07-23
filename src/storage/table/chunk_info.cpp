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

ChunkVectorInfo::ChunkVectorInfo(FixedSizeAllocator &allocator_p, idx_t start, transaction_t insert_id_p)
    : ChunkVectorInfo(allocator_p, start, insert_id_p, NOT_DELETED_ID) {
}

ChunkVectorInfo::ChunkVectorInfo(FixedSizeAllocator &allocator_p, idx_t start, transaction_t insert_id_p,
                                 transaction_t delete_id_p)
    : start(start), allocator(allocator_p), constant_insert_id(insert_id_p), constant_delete_id(delete_id_p) {
}

idx_t ChunkVectorInfo::GetRowCount(ScanOptions options, idx_t max_count) {
	return GetSelVector(options, nullptr, max_count);
}

ChunkVectorInfo::~ChunkVectorInfo() {
	FreeDeleteData();
	if (!HasConstantInsertionId()) {
		allocator.Free(inserted_data);
	}
}

template <class INSERT_OP, class DELETE_OP>
idx_t ChunkVectorInfo::TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
                                             optional_ptr<SelectionVector> sel_vector, idx_t max_count) const {
	if (HasConstantDeleteId()) {
		// all tuples have the same deleted id
		if (DELETE_OP::IsDeleted(start_time, transaction_id, ConstantDeleteId())) {
			// all tuples are deleted
			return 0;
		}
		// no tuples are deleted: we only have to check the inserted ids
		if (HasConstantInsertionId()) {
			// all tuples have the same inserted id as well
			if (INSERT_OP::UseInsertedVersion(start_time, transaction_id, ConstantInsertId())) {
				return max_count;
			} else {
				return 0;
			}
		}
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
	if (HasConstantInsertionId()) {
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
		if (options.delete_type == DeletedScanType::INCLUDE_ALL_DELETED) {
			// include all rows
			return max_count;
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
	if (HasConstantDeleteId()) {
		fetch_deleted_id = ConstantDeleteId();
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
	if (HasConstantDeleteId()) {
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
	if (HasConstantDeleteId()) {
		transaction_t constant_id = ConstantDeleteId();

		deleted_data = allocator.New();
		deleted_data.SetMetadata(1);
		auto segment = allocator.GetHandle(deleted_data);
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			deleted[i] = constant_id;
		}
	}
	return deleted_data;
}

void ChunkVectorInfo::FreeDeleteData() {
	if (HasConstantDeleteId()) {
		return;
	}
	allocator.Free(deleted_data);
	deleted_data = IndexPointer();
}

static bool DeletesEntireVector(const row_t rows[], idx_t count) {
	D_ASSERT(count == STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		if (rows[i] != row_t(i)) {
			return false;
		}
	}
	return true;
}

idx_t ChunkVectorInfo::Delete(transaction_t transaction_id, row_t rows[], idx_t count) {
	if (HasConstantDeleteId() && ConstantDeleteId() != NOT_DELETED_ID) {
		// all rows in this vector share the same deleted id - the rows we are trying to delete are already deleted
		if (ConstantDeleteId() == transaction_id) {
			// the rows were deleted by this transaction already - skip
			return 0;
		}
		// the rows were deleted by another transaction - conflict
		throw TransactionException("Conflict on tuple deletion!");
	}
	if (HasConstantDeleteId() && count == STANDARD_VECTOR_SIZE && DeletesEntireVector(rows, count)) {
		// no rows were deleted yet and we are deleting the entire vector
		// all rows share the same deleted id - store it as a constant instead of materializing per-row delete ids
		constant_delete_id = transaction_id;
		return count;
	}
	// we are materializing / modifying per-row delete ids - re-arm the compression check
	recheck_compression = true;
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
	if (info.is_consecutive && info.count == STANDARD_VECTOR_SIZE) {
		// the delete covers the entire vector - all rows share the same deleted id
		// we can store the deleted id as a constant and free any per-row delete ids
		FreeDeleteData();
		constant_delete_id = commit_id;
		return;
	}
	if (HasConstantDeleteId() && ConstantDeleteId() == commit_id) {
		// all rows already share this exact deleted id - nothing to do
		return;
	}
	// we are materializing / modifying per-row delete ids - re-arm the compression check
	recheck_compression = true;
	bool all_equal = true;
	{
		auto segment = allocator.GetHandle(GetInitializedDeletedPointer());
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
		// check if all rows now share the same deleted id
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			if (deleted[i] != commit_id) {
				all_equal = false;
				break;
			}
		}
	}
	if (all_equal) {
		// all rows share the same deleted id - compress the per-row delete ids into a constant
		FreeDeleteData();
		constant_delete_id = commit_id;
	}
}

void ChunkVectorInfo::VerifyCachedCompressionState() const {
#ifdef DEBUG
	if (recheck_compression) {
		// armed - the next pass re-derives everything from the ids, there is no cached claim to verify
		return;
	}
	// a disarmed check claims that nothing can compress, now or as older transactions finish, until a
	// modification re-arms it. Both conditions below are independent of the lowest active start:
	// per-row insert ids all become visible (or are reverted) eventually, so they must already be compressed
	D_ASSERT(HasConstantInsertionId());
	if (!HasConstantDeleteId()) {
		// per-row delete ids may only remain because a live row blocks the collapse
		auto segment = allocator.GetHandle(GetDeletedPointer());
		auto deleted = segment.GetPtr<transaction_t>();
		bool rows_alive = false;
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			if (deleted[i] == NOT_DELETED_ID) {
				rows_alive = true;
				break;
			}
		}
		D_ASSERT(rows_alive);
	}
#endif
}

VersionCompressionResult ChunkVectorInfo::CompressVersionIds(transaction_t lowest_active_start) {
	if (!recheck_compression) {
		// no ids were modified since this vector last settled - only a further modification
		// (which re-arms the check) can make it compressible, so skip re-scanning the ids
#ifdef DEBUG
		VerifyCachedCompressionState();
#endif
		return HasConstantDeleteId() && HasConstantInsertionId() ? VersionCompressionResult::FULLY_COMPRESSED
		                                                         : VersionCompressionResult::SETTLED;
	}
	bool pending = false;
	if (!HasConstantDeleteId()) {
		// check if all rows are deleted, with all deletes visible to all active and future transactions
		// if so, the per-row delete ids are equivalent to a single constant delete id
		bool rows_alive = false;
		bool deletes_pending = false;
		transaction_t max_delete_id = 0;
		{
			auto segment = allocator.GetHandle(GetDeletedPointer());
			auto deleted = segment.GetPtr<transaction_t>();
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				if (deleted[i] == NOT_DELETED_ID) {
					// the row is not deleted - the ids cannot compress until it is
					rows_alive = true;
				} else if (deleted[i] >= lowest_active_start) {
					// deleted, but the delete is not yet visible to all transactions - the ids can
					// compress once the lowest active start advances past the delete id
					deletes_pending = true;
				} else {
					max_delete_id = MaxValue(max_delete_id, deleted[i]);
				}
			}
		}
		if (!rows_alive && !deletes_pending) {
			FreeDeleteData();
			constant_delete_id = max_delete_id;
		} else if (!rows_alive) {
			pending = true;
		}
	}
	if (!HasConstantInsertionId()) {
		// check if all inserts are visible to all active and future transactions
		// if so, the per-row insert ids are equivalent to a single constant insert id
		bool can_compress = true;
		{
			auto segment = allocator.GetHandle(GetInsertedPointer());
			auto inserted = segment.GetPtr<transaction_t>();
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				if (inserted[i] >= lowest_active_start) {
					// the insert is not yet visible to all transactions
					can_compress = false;
					break;
				}
			}
		}
		if (can_compress) {
			allocator.Free(inserted_data);
			inserted_data = IndexPointer();
			constant_insert_id = 0;
		} else {
			// insert ids become visible to all transactions (or are reverted) eventually
			pending = true;
		}
	}
	recheck_compression = pending;
	if (HasConstantDeleteId() && HasConstantInsertionId()) {
		return VersionCompressionResult::FULLY_COMPRESSED;
	}
	return pending ? VersionCompressionResult::PENDING : VersionCompressionResult::SETTLED;
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

	// we are materializing / modifying per-row insert ids - re-arm the compression check
	recheck_compression = true;
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
	// we are modifying per-row insert ids - re-arm the compression check
	recheck_compression = true;
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
	if (HasConstantInsertionId() && ConstantInsertId() >= TRANSACTION_ID_START) {
		// the vector was inserted by a transaction that has not committed yet
		// the rows have to be masked as deleted when writing a checkpoint
		return true;
	}
	if (!AnyDeleted()) {
		return false;
	}
	if (transaction_id == MAX_TRANSACTION_ID) {
		return true;
	}
	if (HasConstantDeleteId()) {
		return ConstantDeleteId() <= transaction_id;
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

bool ChunkVectorInfo::HasUncommittedChanges() const {
	if (HasConstantInsertionId()) {
		if (ConstantInsertId() >= TRANSACTION_ID_START) {
			return true;
		}
	} else {
		auto insert_segment = allocator.GetHandle(GetInsertedPointer());
		auto inserted = insert_segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			if (inserted[i] >= TRANSACTION_ID_START) {
				return true;
			}
		}
	}
	if (HasConstantDeleteId()) {
		return ConstantDeleteId() != NOT_DELETED_ID && ConstantDeleteId() >= TRANSACTION_ID_START;
	}
	auto delete_segment = allocator.GetHandle(GetDeletedPointer());
	auto deleted = delete_segment.GetPtr<transaction_t>();
	for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
		if (deleted[i] != NOT_DELETED_ID && deleted[i] >= TRANSACTION_ID_START) {
			return true;
		}
	}
	return false;
}

bool ChunkVectorInfo::AnyDeleted() const {
	if (HasConstantDeleteId()) {
		return ConstantDeleteId() != NOT_DELETED_ID;
	}
	return true;
}

bool ChunkVectorInfo::HasConstantInsertionId() const {
	return !inserted_data.HasMetadata();
}

bool ChunkVectorInfo::HasConstantDeleteId() const {
	return !deleted_data.HasMetadata();
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
	if (HasConstantDeleteId()) {
		if (ConstantDeleteId() != NOT_DELETED_ID) {
			result += ", Delete Id: " + to_string(constant_delete_id);
		}
	} else {
		result += ", Delete Ids: [";
		auto segment = allocator.GetHandle(GetDeletedPointer());
		auto deleted = segment.GetPtr<transaction_t>();

		for (idx_t idx = 0; idx < max_count; idx++) {
			if (idx > 0) {
				result += ", ";
			}
			result += to_string(deleted[idx]);
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

transaction_t ChunkVectorInfo::ConstantDeleteId() const {
	if (!HasConstantDeleteId()) {
		throw InternalException("ConstantDeleteId() called but vector info does not have a constant delete id");
	}
	return constant_delete_id;
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
	writer.Write<ChunkInfoType>(ChunkInfoType::VECTOR_INFO);
	writer.Write<idx_t>(start);
	ValidityMask mask(STANDARD_VECTOR_SIZE);
	mask.Initialize(STANDARD_VECTOR_SIZE);
	for (idx_t i = 0; i < count; i++) {
		mask.SetInvalid(sel.get_index(i));
	}
	mask.Write(writer, STANDARD_VECTOR_SIZE);
}

unique_ptr<ChunkVectorInfo> ChunkVectorInfo::Read(FixedSizeAllocator &allocator, ReadStream &reader) {
	auto type = reader.Read<ChunkInfoType>();
	switch (type) {
	case ChunkInfoType::EMPTY_INFO:
		// no rows are deleted
		return nullptr;
	case ChunkInfoType::CONSTANT_INFO: {
		// a fully deleted vector - the constant insert and delete ids of 0 are visible to all transactions
		auto start = reader.Read<idx_t>();
		auto result = make_uniq<ChunkVectorInfo>(allocator, start, 0, 0);
		// both ids are constant - there is nothing left to compress
		result->recheck_compression = false;
		return result;
	}
	case ChunkInfoType::VECTOR_INFO: {
		// a partially deleted vector - the deleted rows are stored as a boolean mask
		auto start = reader.Read<idx_t>();
		auto result = make_uniq<ChunkVectorInfo>(allocator, start);
		ValidityMask mask;
		mask.Read(reader, STANDARD_VECTOR_SIZE);

		bool rows_alive = false;
		auto segment = allocator.GetHandle(result->GetInitializedDeletedPointer());
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			if (mask.RowIsValid(i)) {
				deleted[i] = 0;
			} else {
				rows_alive = true;
			}
		}
		// the reconstructed ids are all visible to all transactions, so the vector can only be
		// compressible if every row is deleted (in which case Write emits CONSTANT_INFO instead,
		// so with the current format the ids are always settled here)
		result->recheck_compression = !rows_alive;
		return result;
	}
	default:
		throw SerializationException("Could not deserialize Chunk Info Type: unrecognized type");
	}
}

} // namespace duckdb
