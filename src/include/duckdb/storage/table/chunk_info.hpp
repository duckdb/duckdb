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

namespace duckdb {
class RowGroup;
struct SelectionVector;
class Transaction;

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
	virtual idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) = 0;
	//! Returns whether or not a single row in the ChunkInfo should be used or not for the given transaction
	virtual bool Fetch(Transaction &transaction, row_t row) = 0;
	virtual void CommitAppend(transaction_t commit_id, idx_t start, idx_t end) = 0;

	virtual void Serialize(Serializer &serialize) = 0;
	static unique_ptr<ChunkInfo> Deserialize(Deserializer &source);
};

class ChunkConstantInfo : public ChunkInfo {
public:
	ChunkConstantInfo(idx_t start);

	transaction_t insert_id;
	transaction_t delete_id;

public:
	idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) override;
	bool Fetch(Transaction &transaction, row_t row) override;
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t end) override;

	void Serialize(Serializer &serialize) override;
	static unique_ptr<ChunkInfo> Deserialize(Deserializer &source);
};

class ChunkVectorInfo : public ChunkInfo {
public:
	ChunkVectorInfo(idx_t start);

	//! The transaction ids of the transactions that inserted the tuples (if any)
	transaction_t inserted[STANDARD_VECTOR_SIZE];
	transaction_t insert_id;
	bool same_inserted_id;

	//! The transaction ids of the transactions that deleted the tuples (if any)
	transaction_t deleted[STANDARD_VECTOR_SIZE];
	bool any_deleted;

public:
	idx_t GetSelVector(transaction_t start_time, transaction_t transaction_id, SelectionVector &sel_vector,
	                   idx_t max_count);
	idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) override;
	bool Fetch(Transaction &transaction, row_t row) override;
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t end) override;

	void Append(idx_t start, idx_t end, transaction_t commit_id);
	void Delete(Transaction &transaction, row_t rows[], idx_t count);
	void CommitDelete(transaction_t commit_id, row_t rows[], idx_t count);

	void Serialize(Serializer &serialize) override;
	static unique_ptr<ChunkInfo> Deserialize(Deserializer &source);
};

} // namespace duckdb
