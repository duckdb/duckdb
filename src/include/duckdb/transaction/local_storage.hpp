//===----------------------------------------------------------------------===//
//                         DuckDB
//
// transaction/local_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/chunk_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/index.hpp"

namespace duckdb {
class DataTable;
class WriteAheadLog;
struct TableAppendState;

class LocalTableStorage {
public:
	LocalTableStorage(DataTable &table);
	~LocalTableStorage();

	//! The main chunk collection holding the data
	ChunkCollection collection;
	//! The set of unique indexes
	vector<unique_ptr<Index>> indexes;
	//! The set of deleted entries
	unordered_map<index_t, unique_ptr<bool[]>> deleted_entries;
	//! The append struct, used during the final append of the LocalTableStorage to the base DataTable
	unique_ptr<TableAppendState> state;
	//! The max row
	row_t max_row;

public:
	void InitializeScan(LocalScanState &state);

	void Clear();
};

//! The LocalStorage class holds appends that have not been committed yet
class LocalStorage {
public:
	//! Initialize a scan of the local storage
	void InitializeScan(DataTable *table, LocalScanState &state);
	//! Scan
	void Scan(LocalScanState &state, const vector<column_t> &column_ids, DataChunk &result);

	//! Append a chunk to the local storage
	void Append(DataTable *table, DataChunk &chunk);
	//! Delete a set of rows from the local storage
	void Delete(DataTable *table, Vector &row_identifiers);
	//! Update a set of rows in the local storage
	void Update(DataTable *table, Vector &row_identifiers, vector<column_t> &column_ids, DataChunk &data);

	//! Check whether or not committing the local storage is possible, throws an exception if it is not possible
	void CheckCommit();

	//! Commits the local storage, writing it to the WAL and completing the commit
	void Commit(Transaction &transaction, WriteAheadLog *log, transaction_t commit_id) noexcept;

	bool ChangesMade() noexcept {
		return table_storage.size() > 0;
	}

private:
	LocalTableStorage *GetStorage(DataTable *table);

	template <class T> bool ScanTableStorage(DataTable *table, LocalTableStorage *storage, T &&fun);

private:
	unordered_map<DataTable *, unique_ptr<LocalTableStorage>> table_storage;
};

} // namespace duckdb
