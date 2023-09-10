//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/chunk_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/vector_size.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {
class RowGroup;
struct SelectionVector;
class Transaction;
struct TransactionData;

class Serializer;
class Deserializer;

enum class ChunkInfoType : uint8_t { CONSTANT_INFO, VECTOR_INFO, EMPTY_INFO };

class ChunkInfo {
public:
	ChunkInfo(idx_t start, ChunkInfoType type) : start(start), type(type) {
	}
	virtual ~ChunkInfo() {
	}

	//! The row index of the first row
	idx_t start;
	//! The ChunkInfo type
	ChunkInfoType type;

public:
	//! Gets up to max_count entries from the chunk info. If the ret is 0>ret>max_count, the selection vector is filled
	//! with the tuples
	virtual idx_t GetSelVector(TransactionData transaction, SelectionVector &sel_vector, idx_t max_count) = 0;
	virtual idx_t GetCommittedSelVector(transaction_t min_start_id, transaction_t min_transaction_id,
	                                    SelectionVector &sel_vector, idx_t max_count) = 0;
	//! Returns whether or not a single row in the ChunkInfo should be used or not for the given transaction
	virtual bool Fetch(TransactionData transaction, row_t row) = 0;
	virtual void CommitAppend(transaction_t commit_id, idx_t start, idx_t end) = 0;
	virtual idx_t GetCommittedDeletedCount(idx_t max_count) = 0;

	virtual bool HasDeletes() const = 0;

	virtual void Write(WriteStream &writer) const;
	static unique_ptr<ChunkInfo> Read(ReadStream &reader);

public:
	template <class TARGET>
	TARGET &Cast() {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast chunk info to type - query result type mismatch");
		}
		return reinterpret_cast<TARGET &>(*this);
	}

	template <class TARGET>
	const TARGET &Cast() const {
		if (type != TARGET::TYPE) {
			throw InternalException("Failed to cast chunk info to type - query result type mismatch");
		}
		return reinterpret_cast<const TARGET &>(*this);
	}
};

class ChunkConstantInfo : public ChunkInfo {
public:
	static constexpr const ChunkInfoType TYPE = ChunkInfoType::CONSTANT_INFO;

public:
	explicit ChunkConstantInfo(idx_t start);

	transaction_t insert_id;
	transaction_t delete_id;

public:
	idx_t GetSelVector(TransactionData transaction, SelectionVector &sel_vector, idx_t max_count) override;
	idx_t GetCommittedSelVector(transaction_t min_start_id, transaction_t min_transaction_id,
	                            SelectionVector &sel_vector, idx_t max_count) override;
	bool Fetch(TransactionData transaction, row_t row) override;
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t end) override;
	idx_t GetCommittedDeletedCount(idx_t max_count) override;

	bool HasDeletes() const override;

	void Write(WriteStream &writer) const override;
	static unique_ptr<ChunkInfo> Read(ReadStream &reader);

private:
	template <class OP>
	idx_t TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id, SelectionVector &sel_vector,
	                            idx_t max_count) const;
};

class ChunkVectorInfo : public ChunkInfo {
public:
	static constexpr const ChunkInfoType TYPE = ChunkInfoType::VECTOR_INFO;

public:
	explicit ChunkVectorInfo(idx_t start);

	//! The transaction ids of the transactions that inserted the tuples (if any)
	transaction_t inserted[STANDARD_VECTOR_SIZE];
	transaction_t insert_id;
	bool same_inserted_id;

	//! The transaction ids of the transactions that deleted the tuples (if any)
	transaction_t deleted[STANDARD_VECTOR_SIZE];
	bool any_deleted;

public:
	idx_t GetSelVector(transaction_t start_time, transaction_t transaction_id, SelectionVector &sel_vector,
	                   idx_t max_count) const;
	idx_t GetSelVector(TransactionData transaction, SelectionVector &sel_vector, idx_t max_count) override;
	idx_t GetCommittedSelVector(transaction_t min_start_id, transaction_t min_transaction_id,
	                            SelectionVector &sel_vector, idx_t max_count) override;
	bool Fetch(TransactionData transaction, row_t row) override;
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t end) override;
	idx_t GetCommittedDeletedCount(idx_t max_count) override;

	void Append(idx_t start, idx_t end, transaction_t commit_id);

	//! Performs a delete in the ChunkVectorInfo - returns how many tuples were actually deleted
	//! The number of rows that were actually deleted might be lower than the input count
	//! In case we delete rows that were already deleted
	//! Note that "rows" is written to to reflect the row ids that were actually deleted
	//! i.e. after calling this function, rows will hold [0..actual_delete_count] row ids of the actually deleted tuples
	idx_t Delete(transaction_t transaction_id, row_t rows[], idx_t count);
	void CommitDelete(transaction_t commit_id, row_t rows[], idx_t count);

	bool HasDeletes() const override;

	void Write(WriteStream &writer) const override;
	static unique_ptr<ChunkInfo> Read(ReadStream &reader);

private:
	template <class OP>
	idx_t TemplatedGetSelVector(transaction_t start_time, transaction_t transaction_id, SelectionVector &sel_vector,
	                            idx_t max_count) const;
};

} // namespace duckdb
