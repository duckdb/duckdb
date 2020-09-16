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
class MorselInfo;
struct SelectionVector;
class Transaction;

enum class ChunkInfoType : uint8_t {
	CONSTANT_INFO,
	DELETE_INFO,
	INSERT_INFO
};

class ChunkInfo {
public:
	ChunkInfo(MorselInfo &morsel, ChunkInfoType type)
	    : morsel(morsel), type(type) {
	}
	virtual ~ChunkInfo() {
	}

	MorselInfo &morsel;
	//! The ChunkInfo type
	ChunkInfoType type;

public:
	//! Gets up to max_count entries from the chunk info. If the ret is 0>ret>max_count, the selection vector is filled
	//! with the tuples
	virtual idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) = 0;
	//! Returns whether or not a single row in the ChunkInfo should be used or not for the given transaction
	virtual bool Fetch(Transaction &transaction, row_t row) = 0;
};


class ChunkConstantInfo : public ChunkInfo {
public:
	ChunkConstantInfo(MorselInfo &morsel);

	transaction_t insert_id;
	transaction_t delete_id;
public:
	idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) override;
	bool Fetch(Transaction &transaction, row_t row) override;
};

// class ChunkDeleteInfo : public ChunkInfo {
// public:
// 	ChunkDeleteInfo(VersionManager &manager, idx_t start_row, ChunkInfoType type = ChunkInfoType::DELETE_INFO);
// 	ChunkDeleteInfo(ChunkDeleteInfo &info, ChunkInfoType type);

// public:
// 	idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) override;
// 	bool Fetch(Transaction &transaction, row_t row) override;

// 	void Delete(Transaction &transaction, row_t rows[], idx_t count) override;
// 	void CommitDelete(transaction_t commit_id, row_t rows[], idx_t count) override;

// protected:
// 	//! The transaction ids of the transactions that deleted the tuples (if any)
// 	transaction_t deleted[STANDARD_VECTOR_SIZE];

// 	bool any_deleted;
// };

class ChunkInsertInfo : public ChunkInfo {
public:
	ChunkInsertInfo(MorselInfo &morsel);

	//! The transaction ids of the transactions that inserted the tuples (if any)
	transaction_t inserted[STANDARD_VECTOR_SIZE];
	transaction_t same_id;
	bool all_same_id;
public:
	idx_t GetSelVector(Transaction &transaction, SelectionVector &sel_vector, idx_t max_count) override;
	bool Fetch(Transaction &transaction, row_t row) override;

	void Append(idx_t start, idx_t end, transaction_t commit_id);
};

} // namespace duckdb
