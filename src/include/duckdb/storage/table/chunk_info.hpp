//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/chunk_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {
struct SelectionVector;
class Transaction;
class VersionManager;

enum class ChunkInfoType : uint8_t { DELETE_INFO, INSERT_INFO };

class ChunkInfo {
public:
	ChunkInfo(VersionManager &manager, idx_t start_row, ChunkInfoType type)
	    : manager(manager), start(start_row), type(type) {
	}
	virtual ~ChunkInfo() {
	}

	//! The version manager
	VersionManager &manager;
	//! The start row of the chunk
	idx_t start;
	//! The ChunkInfo type
	ChunkInfoType type;

public:
	virtual idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) = 0;
	//! Returns whether or not a single row in the ChunkInfo should be used or not for the given transaction
	virtual bool Fetch(Transaction &transaction, row_t row) = 0;
	//! Marks the set of tuples inside the chunk as deleted
	virtual void Delete(Transaction &transaction, row_t rows[], idx_t count) = 0;
	//! Mark the rows as committed
	virtual void CommitDelete(transaction_t commit_id, row_t rows[], idx_t count) = 0;
};

class ChunkDeleteInfo : public ChunkInfo {
public:
	ChunkDeleteInfo(VersionManager &manager, idx_t start_row, ChunkInfoType type = ChunkInfoType::DELETE_INFO);
	ChunkDeleteInfo(ChunkDeleteInfo &info, ChunkInfoType type);

	//! The transaction ids of the transactions that deleted the tuples (if any)
	transaction_t deleted[STANDARD_VECTOR_SIZE];

public:
	idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) override;
	bool Fetch(Transaction &transaction, row_t row) override;

	void Delete(Transaction &transaction, row_t rows[], idx_t count) override;
	void CommitDelete(transaction_t commit_id, row_t rows[], idx_t count) override;
};

class ChunkInsertInfo : public ChunkDeleteInfo {
public:
	ChunkInsertInfo(VersionManager &manager, idx_t start_row);
	ChunkInsertInfo(ChunkDeleteInfo &info);

	//! The transaction ids of the transactions that inserted the tuples (if any)
	transaction_t inserted[STANDARD_VECTOR_SIZE];

public:
	idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) override;
	bool Fetch(Transaction &transaction, row_t row) override;
};

} // namespace duckdb
