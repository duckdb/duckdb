//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/version_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/types/vector.hpp"

#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/storage_lock.hpp"

namespace duckdb {
class DataTable;
class Transaction;
class VersionManager;

class VersionManager {
public:
	VersionManager(DataTable &table) : table(table), max_row(0), base_row(0) {
	}

	//! The DataTable
	DataTable &table;
	//! The read/write lock for the delete info and insert info
	StorageLock lock;
	//! The info for each of the chunks
	unordered_map<index_t, unique_ptr<ChunkInfo>> info;
	//! The maximum amount of rows managed by the version manager
	index_t max_row;
	//! The base row of the version manager, i.e. when passing row = base_row, it will be treated as row = 0
	index_t base_row;

public:
	//! For a given chunk index, fills the selection vector with the relevant tuples for a given transaction. If count
	//! == max_count, all tuples are relevant and the selection vector is not set
	index_t GetSelVector(Transaction &transaction, index_t index, sel_t sel_vector[], index_t max_count);

	//! Fetch a specific row from the VersionManager, returns true if the row should be used for the transaction and
	//! false otherwise.
	bool Fetch(Transaction &transaction, index_t row);

	//! Delete the given set of rows in the version manager
	void Delete(Transaction &transaction, Vector &row_ids);
	//! Append a set of rows to the version manager, setting their inserted id to the given commit_id
	void Append(Transaction &transaction, row_t row_start, index_t count, transaction_t commit_id);
	//! Revert a set of appends made to the version manager from the rows [row_start] until [row_end]
	void RevertAppend(row_t row_start, row_t row_end);

private:
	ChunkInsertInfo *GetInsertInfo(index_t chunk_idx);
};

} // namespace duckdb
