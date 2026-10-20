#include "duckdb/storage/table/chunk_info.hpp"

#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/transaction_data.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/serializer/deserializer.hpp"
#include "duckdb/common/serializer/memory_stream.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"

namespace duckdb {

struct StandardInsertOperator {
	static bool UseInsertedVersion(const SnapshotView &view, transaction_t id) {
		return view.Sees(id);
	}
};

struct IncludeAllInsertedOperator {
	static bool UseInsertedVersion(const SnapshotView &view, transaction_t id) {
		return true;
	}
};

struct StandardDeleteOperator {
	static bool IsDeleted(const SnapshotView &view, transaction_t id) {
		return view.Sees(id);
	}
};

struct CommittedDeleteOperator {
	static bool IsDeleted(const SnapshotView &view, transaction_t id) {
		// check if this row was deleted before the given bound, regardless of who deleted it
		return id < view.visibility_bound;
	}
};

struct IncludeAllDeletedOperator {
	static bool IsDeleted(const SnapshotView &view, transaction_t id) {
		return false;
	}
};

static bool UseVersion(TransactionData transaction, transaction_t id) {
	return transaction.view.Sees(id);
}

static transaction_t UpdateMarker(transaction_t writer) {
	return writer | UPDATE_MARKER_BIT;
}

//! The transaction (or commit) that wrote a delete id or an update marker
static transaction_t GetWriter(transaction_t id) {
	return id & ~UPDATE_MARKER_BIT;
}

static bool IsUpdateMarker(transaction_t id) {
	return id >= UPDATE_MARKER_BIT && id < NOT_DELETED_ID;
}

//! Whether the id marks the row as deleted (it is neither NOT_DELETED_ID nor an update marker)
static bool IsDeleteId(transaction_t id) {
	return id < UPDATE_MARKER_BIT;
}

//! Whether a write of a row conflicts with its id: a delete or update by a transaction that the writer does not see
static bool IsConflictingId(TransactionData transaction, transaction_t id) {
	return id != NOT_DELETED_ID && !transaction.view.Sees(GetWriter(id));
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
idx_t ChunkVectorInfo::TemplatedGetSelVector(const SnapshotView &view, optional_ptr<SelectionVector> sel_vector,
                                             idx_t max_count) const {
	switch (delete_state) {
	case DeleteIdState::CONSTANT: {
		// all tuples have the same deleted id
		if (DELETE_OP::IsDeleted(view, ConstantDeleteId())) {
			// all tuples are deleted
			return 0;
		}
		// no tuples are deleted: we only have to check the inserted ids
		if (HasConstantInsertionId()) {
			// all tuples have the same inserted id as well
			if (INSERT_OP::UseInsertedVersion(view, ConstantInsertId())) {
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
			if (!INSERT_OP::UseInsertedVersion(view, inserted[i])) {
				continue;
			}
			if (sel_vector) {
				sel_vector->set_index(count, i);
			}
			count++;
		}
		return count;
	}
	case DeleteIdState::MASKED: {
		// every deleted row shares mask_delete_id and alive rows are NOT_DELETED_ID (never deleted), so the
		// delete decision is a single constant for the whole vector
		const bool masked_deleted = DELETE_OP::IsDeleted(view, mask_delete_id);
		if (HasConstantInsertionId()) {
			if (!INSERT_OP::UseInsertedVersion(view, ConstantInsertId())) {
				return 0;
			}
			if (!masked_deleted) {
				// the delete is not visible to this transaction - every row is visible
				return max_count;
			}
			// only the alive (mask-invalid) rows are visible
			if (!sel_vector) {
				return max_count - deleted_mask.CountValid(max_count);
			}
			// scan the mask a word at a time: skip fully-deleted words, take fully-alive words wholesale,
			// only extract bits for mixed words
			idx_t count = 0;
			const idx_t entry_count = ValidityMask::EntryCount(max_count);
			for (idx_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
				auto entry = deleted_mask.GetValidityEntry(entry_idx);
				if (ValidityMask::AllValid(entry)) {
					// every row in this word is deleted - skip
					continue;
				}
				const idx_t base = entry_idx * ValidityMask::BITS_PER_VALUE;
				const idx_t entry_end = MinValue<idx_t>(base + ValidityMask::BITS_PER_VALUE, max_count);
				if (ValidityMask::NoneValid(entry)) {
					// every row in this word is alive - select them all
					for (idx_t i = base; i < entry_end; i++) {
						sel_vector->set_index(count++, i);
					}
					continue;
				}
				for (idx_t i = base; i < entry_end; i++) {
					if (!ValidityMask::RowIsValid(entry, i - base)) {
						sel_vector->set_index(count++, i);
					}
				}
			}
			return count;
		}
		// per-row insert ids: the mask cannot collapse the insert check, but the delete decision is still
		// the constant masked_deleted
		auto insert_segment = allocator.GetHandle(GetInsertedPointer());
		auto inserted = insert_segment.GetPtr<transaction_t>();
		idx_t count = 0;
		for (idx_t i = 0; i < max_count; i++) {
			if (!INSERT_OP::UseInsertedVersion(view, inserted[i])) {
				continue;
			}
			if (masked_deleted && deleted_mask.RowIsValid(i)) {
				// the row is deleted and the delete is visible to this transaction
				continue;
			}
			if (sel_vector) {
				sel_vector->set_index(count, i);
			}
			count++;
		}
		return count;
	}
	case DeleteIdState::ARRAY: {
		if (HasConstantInsertionId()) {
			if (!INSERT_OP::UseInsertedVersion(view, ConstantInsertId())) {
				return 0;
			}
			// have to check deleted flag
			idx_t count = 0;
			auto segment = allocator.GetHandle(GetDeletedPointer());
			auto deleted = segment.GetPtr<transaction_t>();
			for (idx_t i = 0; i < max_count; i++) {
				if (DELETE_OP::IsDeleted(view, deleted[i])) {
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
			if (!INSERT_OP::UseInsertedVersion(view, inserted[i])) {
				continue;
			}
			if (DELETE_OP::IsDeleted(view, deleted[i])) {
				continue;
			}
			if (sel_vector) {
				sel_vector->set_index(count, i);
			}
			count++;
		}
		return count;
	}
	default:
		throw InternalException("Unknown DeleteIdState in TemplatedGetSelVector");
	}
}

idx_t ChunkVectorInfo::GetSelVector(ScanOptions options, optional_ptr<SelectionVector> sel_vector,
                                    idx_t max_count) const {
	auto &transaction = options.transaction;
	if (options.insert_type == InsertedScanType::STANDARD) {
		if (options.delete_type == DeletedScanType::STANDARD) {
			return TemplatedGetSelVector<StandardInsertOperator, StandardDeleteOperator>(transaction.view, sel_vector,
			                                                                             max_count);
		}
		if (options.delete_type == DeletedScanType::INCLUDE_ALL_DELETED) {
			return TemplatedGetSelVector<StandardInsertOperator, IncludeAllDeletedOperator>(transaction.view,
			                                                                                sel_vector, max_count);
		}
		if (options.delete_type == DeletedScanType::OMIT_COMMITTED_DELETES) {
			return TemplatedGetSelVector<StandardInsertOperator, CommittedDeleteOperator>(transaction.view, sel_vector,
			                                                                              max_count);
		}
	}
	if (options.insert_type == InsertedScanType::ALL_ROWS) {
		if (options.delete_type == DeletedScanType::STANDARD) {
			return TemplatedGetSelVector<IncludeAllInsertedOperator, StandardDeleteOperator>(transaction.view,
			                                                                                 sel_vector, max_count);
		}
		if (options.delete_type == DeletedScanType::INCLUDE_ALL_DELETED) {
			// include all rows
			return max_count;
		}
		if (options.delete_type == DeletedScanType::OMIT_COMMITTED_DELETES) {
			return TemplatedGetSelVector<IncludeAllInsertedOperator, CommittedDeleteOperator>(transaction.view,
			                                                                                  sel_vector, max_count);
		}
	}
	throw InternalException("Unsupported combination of insert / delete types in ChunkVectorInfo::GetSelVector");
}

bool ChunkVectorInfo::Fetch(TransactionData transaction, row_t row) {
	transaction_t fetch_insert_id;
	if (HasConstantInsertionId()) {
		fetch_insert_id = ConstantInsertId();
	} else {
		auto insert_segment = allocator.GetHandle(GetInsertedPointer());
		auto inserted = insert_segment.GetPtr<transaction_t>();
		fetch_insert_id = inserted[row];
	}
	transaction_t fetch_deleted_id;
	switch (delete_state) {
	case DeleteIdState::CONSTANT:
		fetch_deleted_id = ConstantDeleteId();
		break;
	case DeleteIdState::MASKED:
		fetch_deleted_id = deleted_mask.RowIsValid(NumericCast<idx_t>(row)) ? mask_delete_id : NOT_DELETED_ID;
		break;
	case DeleteIdState::ARRAY: {
		auto delete_segment = allocator.GetHandle(GetDeletedPointer());
		fetch_deleted_id = delete_segment.GetPtr<transaction_t>()[row];
		break;
	}
	default:
		throw InternalException("Unknown DeleteIdState in Fetch");
	}

	return UseVersion(transaction, fetch_insert_id) && !UseVersion(transaction, fetch_deleted_id);
}

//! Whether the rows (rows[i] - offset) are exactly the first count rows of the vector
static bool CoversFirstRows(const row_t rows[], idx_t count, row_t offset) {
	for (idx_t i = 0; i < count; i++) {
		if (rows[i] - offset != row_t(i)) {
			return false;
		}
	}
	return true;
}

bool ChunkVectorInfo::HasConflictingWrite(TransactionData transaction, const row_t ids[], idx_t count,
                                          row_t offset) const {
	switch (delete_state) {
	case DeleteIdState::CONSTANT:
		return IsConflictingId(transaction, ConstantDeleteId());
	case DeleteIdState::MASKED:
		// the deleted rows of the mask share mask_delete_id
		if (!IsConflictingId(transaction, mask_delete_id)) {
			return false;
		}
		for (idx_t i = 0; i < count; i++) {
			if (deleted_mask.RowIsValid(NumericCast<idx_t>(ids[i] - offset))) {
				return true;
			}
		}
		return false;
	case DeleteIdState::ARRAY: {
		auto segment = allocator.GetHandle(GetDeletedPointer());
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < count; i++) {
			if (IsConflictingId(transaction, deleted[ids[i] - offset])) {
				return true;
			}
		}
		return false;
	}
	default:
		throw InternalException("Unknown DeleteIdState in HasConflictingWrite");
	}
}

IndexPointer ChunkVectorInfo::GetInsertedPointer() const {
	if (HasConstantInsertionId()) {
		throw InternalException("ChunkVectorInfo: insert id requested but insertions were not initialized");
	}
	return inserted_data;
}

IndexPointer ChunkVectorInfo::GetDeletedPointer() const {
	if (delete_state != DeleteIdState::ARRAY) {
		throw InternalException(
		    "ChunkVectorInfo: deleted id array requested but delete side is not in the ARRAY state");
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
	if (delete_state == DeleteIdState::MASKED) {
		// re-materialize the per-row array so callers can write into it
		DecompressDeleteMask();
	}
	if (HasConstantDeleteId()) {
		transaction_t constant_id = ConstantDeleteId();

		deleted_data = allocator.New();
		deleted_data.SetMetadata(1);
		auto segment = allocator.GetHandle(deleted_data);
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			deleted[i] = constant_id;
		}
		delete_state = DeleteIdState::ARRAY;
	}
	return deleted_data;
}

void ChunkVectorInfo::FreeDeleteData() {
	if (delete_state == DeleteIdState::ARRAY) {
		allocator.Free(deleted_data);
		deleted_data = IndexPointer();
	}
	deleted_mask.Reset();
	delete_state = DeleteIdState::CONSTANT;
}

void ChunkVectorInfo::CompressDeleteToMask(transaction_t mask_id) {
	D_ASSERT(delete_state == DeleteIdState::ARRAY);
	// the mask can only carry a single committed id shared by every deleted row
	D_ASSERT(IsCommitted(mask_id));
	// start all-valid (== all deleted), then mark the alive rows invalid
	deleted_mask.Initialize(STANDARD_VECTOR_SIZE);
	{
		auto segment = allocator.GetHandle(deleted_data);
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			if (deleted[i] == NOT_DELETED_ID) {
				deleted_mask.SetInvalid(i);
			}
		}
	} // release the read handle before freeing the buffer
	allocator.Free(deleted_data);
	deleted_data = IndexPointer();
	mask_delete_id = mask_id;
	delete_state = DeleteIdState::MASKED;
}

void ChunkVectorInfo::DecompressDeleteMask() {
	D_ASSERT(delete_state == DeleteIdState::MASKED);
	// re-materialize the per-row array: deleted rows == mask_delete_id, alive rows == NOT_DELETED_ID
	deleted_data = allocator.New();
	deleted_data.SetMetadata(1);
	{
		auto segment = allocator.GetHandle(deleted_data);
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			deleted[i] = deleted_mask.RowIsValid(i) ? mask_delete_id : NOT_DELETED_ID;
		}
	}
	deleted_mask.Reset();
	delete_state = DeleteIdState::ARRAY;
	recheck_compression = true;
}

idx_t ChunkVectorInfo::WriteRows(TransactionData transaction, const row_t ids[], idx_t count, row_t offset,
                                 bool is_update, uint16_t rows[], transaction_t previous_ids[]) {
	if (HasConflictingWrite(transaction, ids, count, offset)) {
		throw TransactionException(is_update ? "Conflict on update!" : "Conflict on tuple deletion!");
	}
	auto transaction_id = transaction.GetTransactionId();
	auto id = is_update ? UpdateMarker(transaction_id) : transaction_id;
	if (HasConstantDeleteId() && ConstantDeleteId() == id) {
		// the transaction wrote all rows of the vector already
		return 0;
	}
	// the ids are not visible to all transactions yet - re-arm the compression check
	recheck_compression = true;
	if (HasConstantDeleteId() && count == STANDARD_VECTOR_SIZE && CoversFirstRows(ids, count, offset)) {
		// all rows get the same id - store it as a constant instead of materializing per-row ids
		D_ASSERT(!IsDeleteId(constant_delete_id));
		for (idx_t i = 0; i < count; i++) {
			rows[i] = NumericCast<uint16_t>(i);
			previous_ids[i] = constant_delete_id;
		}
		constant_delete_id = id;
		return count;
	}
	auto segment = allocator.GetHandle(GetInitializedDeletedPointer());
	auto deleted = segment.GetPtr<transaction_t>();
	idx_t written = 0;
	for (idx_t i = 0; i < count; i++) {
		auto row = NumericCast<idx_t>(ids[i] - offset);
		if (deleted[row] == id) {
			// the transaction wrote the row already
			continue;
		}
		// a transaction that sees the delete of a row cannot see the row, so it never writes it
		D_ASSERT(!IsDeleteId(deleted[row]));
		rows[written] = NumericCast<uint16_t>(row);
		previous_ids[written] = deleted[row];
		deleted[row] = id;
		written++;
	}
	return written;
}

template <class F>
void ChunkVectorInfo::SetIds(const uint16_t rows[], idx_t count, F &&new_id) {
	recheck_compression = true;
	bool all_equal = true;
	transaction_t first_id;
	{
		auto segment = allocator.GetHandle(GetInitializedDeletedPointer());
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < count; i++) {
			auto row = rows ? rows[i] : i;
			deleted[row] = new_id(deleted[row], i);
		}
		first_id = deleted[0];
		for (idx_t i = 1; i < STANDARD_VECTOR_SIZE; i++) {
			if (deleted[i] != first_id) {
				all_equal = false;
				break;
			}
		}
	}
	if (all_equal) {
		// all rows share the same id - compress the per-row ids into a constant
		FreeDeleteData();
		constant_delete_id = first_id;
	}
}

void ChunkVectorInfo::CommitWrite(transaction_t old_id, transaction_t new_id, const uint16_t rows[], idx_t count) {
	// the transaction's ids are its delete ids and its update markers
	auto replace = [&](transaction_t id, idx_t) {
		return id == old_id ? new_id : id == UpdateMarker(old_id) ? UpdateMarker(new_id) : id;
	};
	if (HasConstantDeleteId()) {
		// all rows hold the same id
		recheck_compression = true;
		constant_delete_id = replace(constant_delete_id, 0);
		return;
	}
	SetIds(rows, count, replace);
}

void ChunkVectorInfo::RollbackWrite(const uint16_t rows[], idx_t count, const transaction_t previous_ids[],
                                    idx_t previous_id_count) {
	D_ASSERT(previous_id_count == 0 || previous_id_count == 1 || previous_id_count == count);
	auto previous_id = [&](transaction_t, idx_t i) {
		return previous_id_count == 0 ? NOT_DELETED_ID : previous_ids[previous_id_count == 1 ? 0 : i];
	};
	if (!rows && count == STANDARD_VECTOR_SIZE && previous_id_count <= 1) {
		// all rows held the same id before
		recheck_compression = true;
		FreeDeleteData();
		constant_delete_id = previous_id(0, 0);
		return;
	}
	SetIds(rows, count, previous_id);
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
	if (delete_state == DeleteIdState::CONSTANT) {
		// update markers become visible to all transactions (or roll back) eventually as well
		D_ASSERT(!IsUpdateMarker(ConstantDeleteId()));
	} else {
		// a settled, non-constant delete side can only be the terminal masked state
		D_ASSERT(delete_state == DeleteIdState::MASKED);
		// the mask must hold at least one deleted row and one alive row: a fully deleted vector
		// collapses to a constant, and a vector with no deletes carries no delete-side info
		idx_t deleted_rows = deleted_mask.CountValid(STANDARD_VECTOR_SIZE);
		D_ASSERT(deleted_rows > 0 && deleted_rows < STANDARD_VECTOR_SIZE);
	}
#endif
}

VersionCompressionResult ChunkVectorInfo::CompressVersionIds(VisibilityBound lowest_visibility_bound) {
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
	if (HasConstantDeleteId() && IsUpdateMarker(ConstantDeleteId())) {
		if (GetWriter(ConstantDeleteId()) < lowest_visibility_bound) {
			// all transactions see the update - no write of the rows can conflict with it anymore
			constant_delete_id = NOT_DELETED_ID;
		} else {
			pending = true;
		}
	}
	if (delete_state == DeleteIdState::ARRAY) {
		// scan the per-row delete ids to decide how far they can collapse
		bool rows_alive = false;
		bool markers_pending = false;
		bool deletes_pending = false;
		bool deletes_uncommitted = false;
		bool deletes_equal = true;
		transaction_t max_delete_id = 0;
		transaction_t shared_delete_id = NOT_DELETED_ID;
		{
			auto segment = allocator.GetHandle(GetDeletedPointer());
			auto deleted = segment.GetPtr<transaction_t>();
			for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
				if (IsUpdateMarker(deleted[i])) {
					if (GetWriter(deleted[i]) < lowest_visibility_bound) {
						// all transactions see the update - drop the marker
						deleted[i] = NOT_DELETED_ID;
					} else {
						markers_pending = true;
					}
				}
				if (!IsDeleteId(deleted[i])) {
					// the row is not deleted - the ids cannot fully collapse until it is
					rows_alive = true;
					continue;
				}
				if (deleted[i] >= lowest_visibility_bound) {
					// deleted, but the delete is not yet visible to all transactions
					deletes_pending = true;
					if (!IsCommitted(deleted[i])) {
						// the delete is not even committed yet - the array must be kept
						deletes_uncommitted = true;
					}
				} else {
					max_delete_id = MaxValue(max_delete_id, deleted[i]);
				}
				// track whether every deleted row shares a single id
				if (shared_delete_id == NOT_DELETED_ID) {
					shared_delete_id = deleted[i];
				} else if (deleted[i] != shared_delete_id) {
					deletes_equal = false;
				}
			}
		}
		if (!rows_alive && !deletes_pending) {
			// entire vector deleted and visible to all - collapse to a constant
			FreeDeleteData();
			constant_delete_id = max_delete_id;
		} else if (!rows_alive) {
			// entire vector deleted but a delete is still pending - retry next pass
			pending = true;
		} else if (markers_pending) {
			// the markers of updates that not all transactions see yet have to stay - retry next pass
			pending = true;
		} else if (shared_delete_id == NOT_DELETED_ID) {
			// only the dropped markers were left - nothing is deleted
			FreeDeleteData();
		} else if (!deletes_pending) {
			// partially deleted, every delete visible to all - compress to a mask (terminal)
			CompressDeleteToMask(0);
		} else if (deletes_equal && !deletes_uncommitted) {
			// partially deleted, every delete committed by the same transaction but not yet visible to
			// all - compress to a mask carrying that single committed id (terminal). Older snapshots still
			// see the rows via the id comparison, and on reload every id is visible so it becomes 0.
			CompressDeleteToMask(shared_delete_id);
		} else {
			// partially deleted with pending deletes from multiple or uncommitted transactions - retry
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
				if (inserted[i] >= lowest_visibility_bound) {
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
	if (HasConstantDeleteId() && HasConstantInsertionId() && !IsUpdateMarker(ConstantDeleteId())) {
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

bool ChunkVectorInfo::Cleanup(VisibilityBound lowest_visibility_bound) const {
	if (!HasConstantDeleteId() || ConstantDeleteId() != NOT_DELETED_ID) {
		// if any rows are deleted (or marked as updated) we can't clean-up
		return false;
	}
	// check if the insertion markers have to be used by all transactions going forward
	if (!HasConstantInsertionId()) {
		auto segment = allocator.GetHandle(GetInsertedPointer());
		auto inserted = segment.GetPtr<transaction_t>();

		// from 1: index 0 holds the first append, which carries the lowest insert id
		for (idx_t idx = 1; idx < STANDARD_VECTOR_SIZE; idx++) {
			if (inserted[idx] >= lowest_visibility_bound) {
				// not yet visible to every current and future snapshot - cannot compress
				return false;
			}
		}
	} else if (ConstantInsertId() >= lowest_visibility_bound) {
		return false;
	}
	return true;
}

bool ChunkVectorInfo::HasDeletes(VisibilityBound visibility_bound) const {
	if (HasConstantInsertionId() && !IsCommitted(ConstantInsertId())) {
		// the vector was inserted by a transaction that has not committed yet
		// the rows have to be masked as deleted when writing a checkpoint
		return true;
	}
	if (!AnyDeleted()) {
		return false;
	}
	if (visibility_bound == VisibilityBound::IncludingUncommitted()) {
		return true;
	}
	switch (delete_state) {
	case DeleteIdState::CONSTANT:
		return ConstantDeleteId() < visibility_bound;
	case DeleteIdState::MASKED:
		// AnyDeleted() above guaranteed at least one deleted row; they all share mask_delete_id
		return mask_delete_id < visibility_bound;
	case DeleteIdState::ARRAY: {
		auto segment = allocator.GetHandle(GetDeletedPointer());
		auto deleted = segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			if (deleted[i] < visibility_bound) {
				return true;
			}
		}
		return false;
	}
	default:
		throw InternalException("Unknown DeleteIdState in HasDeletes");
	}
}

bool ChunkVectorInfo::HasUncommittedChanges() const {
	if (HasConstantInsertionId()) {
		if (!IsCommitted(ConstantInsertId())) {
			return true;
		}
	} else {
		auto insert_segment = allocator.GetHandle(GetInsertedPointer());
		auto inserted = insert_segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			if (!IsCommitted(inserted[i])) {
				return true;
			}
		}
	}
	switch (delete_state) {
	case DeleteIdState::CONSTANT:
		return IsDeleteId(ConstantDeleteId()) && !IsCommitted(ConstantDeleteId());
	case DeleteIdState::MASKED:
		// the mask is only ever folded from committed deletes, so mask_delete_id is always committed
		D_ASSERT(IsCommitted(mask_delete_id));
		return false;
	case DeleteIdState::ARRAY: {
		auto delete_segment = allocator.GetHandle(GetDeletedPointer());
		auto deleted = delete_segment.GetPtr<transaction_t>();
		for (idx_t i = 0; i < STANDARD_VECTOR_SIZE; i++) {
			if (IsDeleteId(deleted[i]) && !IsCommitted(deleted[i])) {
				return true;
			}
		}
		return false;
	}
	default:
		throw InternalException("Unknown DeleteIdState in HasUncommittedChanges");
	}
}

bool ChunkVectorInfo::AnyDeleted() const {
	switch (delete_state) {
	case DeleteIdState::CONSTANT:
		return IsDeleteId(ConstantDeleteId());
	case DeleteIdState::MASKED:
		// a masked vector always contains at least one deleted row
		return true;
	case DeleteIdState::ARRAY:
		return true;
	default:
		throw InternalException("Unknown DeleteIdState in AnyDeleted");
	}
}

bool ChunkVectorInfo::HasConstantInsertionId() const {
	return !inserted_data.HasMetadata();
}

bool ChunkVectorInfo::HasConstantDeleteId() const {
	return delete_state == DeleteIdState::CONSTANT;
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
	switch (delete_state) {
	case DeleteIdState::CONSTANT:
		if (IsUpdateMarker(ConstantDeleteId())) {
			result += ", Update Marker: " + to_string(GetWriter(constant_delete_id));
		} else if (ConstantDeleteId() != NOT_DELETED_ID) {
			result += ", Delete Id: " + to_string(constant_delete_id);
		}
		break;
	case DeleteIdState::MASKED: {
		result += ", Delete Id: " + to_string(mask_delete_id);
		result += ", Deleted (mask): [";
		for (idx_t idx = 0; idx < max_count; idx++) {
			if (idx > 0) {
				result += ", ";
			}
			result += deleted_mask.RowIsValid(idx) ? "1" : "0";
		}
		result += "]";
		break;
	}
	case DeleteIdState::ARRAY: {
		result += ", Delete Ids: [";
		auto segment = allocator.GetHandle(GetDeletedPointer());
		auto deleted = segment.GetPtr<transaction_t>();

		for (idx_t idx = 0; idx < max_count; idx++) {
			if (idx > 0) {
				result += ", ";
			}
			result += IsUpdateMarker(deleted[idx]) ? "U" + to_string(GetWriter(deleted[idx])) : to_string(deleted[idx]);
		}
		result += "]";
		break;
	}
	default:
		throw InternalException("Unknown DeleteIdState in ToString");
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

void ChunkVectorInfo::Write(WriteStream &writer, VisibilityBound visibility_bound) const {
	SelectionVector sel(STANDARD_VECTOR_SIZE);
	transaction_t transaction_id = DConstants::INVALID_INDEX;
	idx_t count = GetSelVector(TransactionData(transaction_id, visibility_bound), sel, STANDARD_VECTOR_SIZE);
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
		auto result = make_uniq<ChunkVectorInfo>(allocator, start, transaction_t(0), transaction_t(0));
		// both ids are constant - there is nothing left to compress
		result->recheck_compression = false;
		return result;
	}
	case ChunkInfoType::VECTOR_INFO: {
		// a partially deleted vector - the deleted rows are stored as a boolean mask, all committed and
		// visible to every transaction. The on-disk orientation (valid == deleted) matches the in-memory
		// MASKED state, so load it straight into deleted_mask without materializing a per-row array.
		auto start = reader.Read<idx_t>();
		auto result = make_uniq<ChunkVectorInfo>(allocator, start);
		result->deleted_mask.Read(reader, STANDARD_VECTOR_SIZE);
		// Write only emits VECTOR_INFO for a partial delete: an all-deleted vector becomes
		// CONSTANT_INFO and an undeleted one becomes EMPTY_INFO, so the mask must have at least one
		// deleted (valid) and one alive (invalid) bit - never all-valid, never all-invalid.
		if (result->deleted_mask.CheckAllValid(STANDARD_VECTOR_SIZE) ||
		    result->deleted_mask.CheckAllInvalid(STANDARD_VECTOR_SIZE)) {
			throw DataCorruptionException(
			    "Partial-delete vector info mask marks either all rows deleted or all rows alive, but a "
			    "VECTOR_INFO block must always encode a partial delete. The database file may be corrupted.");
		}
		result->delete_state = DeleteIdState::MASKED;
		// on-disk deletes are all committed and visible to every transaction - the shared id is 0
		result->mask_delete_id = 0;
		// every id is already visible to all transactions - nothing left to compress
		result->recheck_compression = false;
		return result;
	}
	default:
		throw SerializationException("Could not deserialize Chunk Info Type: unrecognized type");
	}
}

} // namespace duckdb
