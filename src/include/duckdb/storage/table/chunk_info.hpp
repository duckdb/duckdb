//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/chunk_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/index/index_pointer.hpp"
#include "duckdb/common/enums/scan_options.hpp"

namespace duckdb {
class RowGroup;
struct SelectionVector;
class Transaction;
struct TransactionData;
struct DeleteInfo;
class Serializer;
class Deserializer;
class FixedSizeAllocator;

//! The type of a serialized chunk info entry
//! In-memory all chunk infos are represented by ChunkVectorInfo - CONSTANT_INFO (a fully deleted vector) and
//! EMPTY_INFO (no deletes) only exist as tags in the serialized format
enum class ChunkInfoType : uint8_t { CONSTANT_INFO, VECTOR_INFO, EMPTY_INFO };

//! ChunkVectorInfo holds the version information (the insert and delete ids) of the rows of a single vector within
//! a row group. The insert and delete ids are stored as constants when all rows share the same id, and as per-row
//! id arrays otherwise.

//! The result of a CompressVersionIds pass over a vector
enum class VersionCompressionResult : uint8_t {
	//! No per-row id arrays remain - there is nothing left to compress
	FULLY_COMPRESSED,
	//! Some ids are not yet visible to all transactions - compression can succeed once the
	//! lowest active start advances past them, without any further modifications
	PENDING,
	//! Per-row arrays remain that cannot compress without further modifications
	//! (some rows are not deleted)
	SETTLED
};

class ChunkVectorInfo {
public:
	explicit ChunkVectorInfo(FixedSizeAllocator &allocator, idx_t start, transaction_t insert_id = 0);
	ChunkVectorInfo(FixedSizeAllocator &allocator, idx_t start, transaction_t insert_id, transaction_t delete_id);
	~ChunkVectorInfo();

	//! The row index of the first row
	idx_t start;

public:
	//! Gets up to max_count entries from the chunk info. If the ret is 0>ret>max_count, the selection vector is filled
	//! with the tuples
	idx_t GetSelVector(ScanOptions options, optional_ptr<SelectionVector> sel_vector, idx_t max_count) const;
	idx_t GetRowCount(ScanOptions options, idx_t max_count);
	//! Returns whether or not a single row in the ChunkVectorInfo should be used or not for the given transaction
	bool Fetch(TransactionData transaction, row_t row);
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t end);
	bool Cleanup(transaction_t lowest_transaction) const;
	string ToString(idx_t max_count) const;

	void Append(idx_t start, idx_t end, transaction_t commit_id);

	//! Performs a delete in the ChunkVectorInfo - returns how many tuples were actually deleted
	//! The number of rows that were actually deleted might be lower than the input count
	//! In case we delete rows that were already deleted
	//! Note that "rows" is written to to reflect the row ids that were actually deleted
	//! i.e. after calling this function, rows will hold [0..actual_delete_count] row ids of the actually deleted tuples
	idx_t Delete(transaction_t transaction_id, row_t rows[], idx_t count);
	void CommitDelete(transaction_t commit_id, const DeleteInfo &info);

	//! Attempts to compress the per-row insert/delete ids into constants
	//! This is possible when the ids behave identically for all transactions with a start time of at least
	//! lowest_active_start (i.e. all active and future transactions)
	VersionCompressionResult CompressVersionIds(transaction_t lowest_active_start);
	//! Whether a compression pass could achieve anything for this vector (see recheck_compression)
	bool RecheckCompression() const {
		return recheck_compression;
	}
	//! Verifies (in DEBUG) that a disarmed recheck_compression matches the actual ids: nothing may be
	//! compressible now or in the future without a modification that re-arms the check
	void VerifyCachedCompressionState() const;

	bool HasDeletes(transaction_t transaction_id = MAX_TRANSACTION_ID) const;
	bool HasUncommittedChanges() const;
	bool AnyDeleted() const;
	bool HasConstantInsertionId() const;
	transaction_t ConstantInsertId() const;
	bool HasConstantDeleteId() const;
	transaction_t ConstantDeleteId() const;

	void Write(WriteStream &writer, transaction_t transaction_id) const;
	static unique_ptr<ChunkVectorInfo> Read(FixedSizeAllocator &allocator, ReadStream &reader);

private:
	template <class INSERT_OP, class DELETE_OP>
	idx_t TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id,
	                            optional_ptr<SelectionVector> sel_vector, idx_t max_count) const;

	IndexPointer GetInsertedPointer() const;
	IndexPointer GetDeletedPointer() const;
	IndexPointer GetInitializedInsertedPointer();
	IndexPointer GetInitializedDeletedPointer();
	//! Frees the per-row delete ids (if any)
	void FreeDeleteData();

private:
	FixedSizeAllocator &allocator;
	//! The transaction ids of the transactions that inserted the tuples (if any)
	IndexPointer inserted_data;
	//! The constant insert id (if there is only one)
	transaction_t constant_insert_id;

	//! The transaction ids of the transactions that deleted the tuples (if any)
	IndexPointer deleted_data;
	//! The constant delete id (if there is only one, e.g. because the entire vector was deleted in one transaction)
	transaction_t constant_delete_id;
	//! Whether a compression pass could achieve anything for this vector: armed by any id modification,
	//! disarmed when a pass compresses the vector fully or finds it settled (live rows block the collapse
	//! until a further delete re-arms it). CompressVersionIds returns the cached SETTLED without
	//! re-scanning the ids while this is false.
	bool recheck_compression = true;
};

} // namespace duckdb
