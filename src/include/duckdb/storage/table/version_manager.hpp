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

struct DataTableInfo;

class VersionManager {
public:
	VersionManager(DataTableInfo &table_info) : table_info(table_info), max_row(0), base_row(0) {
	}

	//! The DataTableInfo
	DataTableInfo &table_info;
	//! The read/write lock for the delete info and insert info
	StorageLock lock;
	//! The info for each of the chunks
	unordered_map<idx_t, unique_ptr<ChunkInfo>> info;
	//! The maximum amount of rows managed by the version manager
	idx_t max_row;
	//! The base row of the version manager, i.e. when passing row = base_row, it will be treated as row = 0
	idx_t base_row;

public:
	//! For a given chunk index, fills the selection vector with the relevant tuples for a given transaction. If count
	//! == max_count, all tuples are relevant and the selection vector is not set
	idx_t GetSelVector(Transaction &transaction, idx_t index, SelectionVector &sel_vector, idx_t max_count);

	//! Fetch a specific row from the VersionManager, returns true if the row should be used for the transaction and
	//! false otherwise.
	bool Fetch(Transaction &transaction, idx_t row);

	//! Delete the given set of rows in the version manager
	void Delete(Transaction &transaction, DataTable *table, Vector &row_ids, idx_t count);
	//! Append a set of rows to the version manager, setting their inserted id to the given commit_id
	void Append(Transaction &transaction, row_t row_start, idx_t count, transaction_t commit_id);
	//! Revert a set of appends made to the version manager from the rows [row_start] until [row_end]
	void RevertAppend(row_t row_start, row_t row_end);

private:
	ChunkInsertInfo *GetInsertInfo(idx_t chunk_idx);
};

} // namespace duckdb
