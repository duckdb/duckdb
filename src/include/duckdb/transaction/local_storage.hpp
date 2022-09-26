//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/local_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/storage/table/table_statistics.hpp"

namespace duckdb {
class DataTable;
class WriteAheadLog;
struct TableAppendState;

class LocalTableStorage : public std::enable_shared_from_this<LocalTableStorage> {
public:
	// Create a new LocalTableStorage
	explicit LocalTableStorage(DataTable &table);
	// Create a LocalTableStorage from an ALTER TYPE
	LocalTableStorage(DataTable &table, LocalTableStorage &parent, idx_t changed_idx, const LogicalType &target_type,
	                  const vector<column_t> &bound_columns, Expression &cast_expr);
	// Create a LocalTableStorage from a DROP COLUMN
	LocalTableStorage(DataTable &table, LocalTableStorage &parent, idx_t drop_idx);
	// Create a LocalTableStorage from a ADD COLUMN
	LocalTableStorage(DataTable &table, LocalTableStorage &parent, ColumnDefinition &new_column,
	                  Expression *default_value);

	~LocalTableStorage();

	DataTable &table;

	Allocator &allocator;
	//! The main chunk collection holding the data
	shared_ptr<RowGroupCollection> row_groups;
	//! The set of unique indexes
	TableIndexList indexes;
	//! Stats
	TableStatistics stats;
	//! The number of deleted rows
	idx_t deleted_rows;

public:
	void InitializeScan(CollectionScanState &state, TableFilterSet *table_filters = nullptr);
	idx_t EstimatedSize();
};

//! The LocalStorage class holds appends that have not been committed yet
class LocalStorage {
public:
	struct CommitState {
		unordered_map<DataTable *, unique_ptr<TableAppendState>> append_states;
	};

public:
	explicit LocalStorage(Transaction &transaction) : transaction(transaction) {
	}

	//! Initialize a scan of the local storage
	void InitializeScan(DataTable *table, CollectionScanState &state, TableFilterSet *table_filters);
	//! Scan
	void Scan(CollectionScanState &state, const vector<column_t> &column_ids, DataChunk &result);

	void InitializeParallelScan(DataTable *table, ParallelCollectionScanState &state);
	bool NextParallelScan(ClientContext &context, DataTable *table, ParallelCollectionScanState &state,
	                      CollectionScanState &scan_state);

	//! Append a chunk to the local storage
	void Append(DataTable *table, DataChunk &chunk);
	//! Delete a set of rows from the local storage
	idx_t Delete(DataTable *table, Vector &row_ids, idx_t count);
	//! Update a set of rows in the local storage
	void Update(DataTable *table, Vector &row_ids, const vector<column_t> &column_ids, DataChunk &data);

	//! Commits the local storage, writing it to the WAL and completing the commit
	void Commit(LocalStorage::CommitState &commit_state, Transaction &transaction, WriteAheadLog *log,
	            transaction_t commit_id);

	bool ChangesMade() noexcept {
		return table_storage.size() > 0;
	}
	idx_t EstimatedSize();

	bool Find(DataTable *table) {
		return table_storage.find(table) != table_storage.end();
	}

	idx_t AddedRows(DataTable *table);

	void AddColumn(DataTable *old_dt, DataTable *new_dt, ColumnDefinition &new_column, Expression *default_value);
	void DropColumn(DataTable *old_dt, DataTable *new_dt, idx_t removed_column);
	void ChangeType(DataTable *old_dt, DataTable *new_dt, idx_t changed_idx, const LogicalType &target_type,
	                const vector<column_t> &bound_columns, Expression &cast_expr);

	void MoveStorage(DataTable *old_dt, DataTable *new_dt);
	void FetchChunk(DataTable *table, Vector &row_ids, idx_t count, DataChunk &chunk);
	TableIndexList &GetIndexes(DataTable *table);

	void VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint);

private:
	LocalTableStorage *GetStorage(DataTable *table);

	template <class T>
	bool ScanTableStorage(DataTable &table, LocalTableStorage &storage, T &&fun);

private:
	Transaction &transaction;
	unordered_map<DataTable *, shared_ptr<LocalTableStorage>> table_storage;

	void Flush(DataTable &table, LocalTableStorage &storage);
};

} // namespace duckdb
