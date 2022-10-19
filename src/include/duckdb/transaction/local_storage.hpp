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

class OptimisticDataWriter {
public:
	OptimisticDataWriter(DataTable *table);
	OptimisticDataWriter(DataTable *table, OptimisticDataWriter &parent);
	~OptimisticDataWriter();

	void CheckFlushToDisk(RowGroupCollection &row_groups);
	//! Flushes a specific row group to disk
	void FlushToDisk(RowGroup *row_group);
	//! Flushes the final row group to disk (if any)
	void FlushToDisk(RowGroupCollection &row_groups);
	//! Final flush: flush the partial block manager to disk
	void FinalFlush();

	void Rollback();

private:
	//! The table
	DataTable *table;
	//! The partial block manager (if we created one yet)
	unique_ptr<PartialBlockManager> partial_manager;
	//! The set of blocks that have been pre-emptively written to disk
	unordered_set<block_id_t> written_blocks;
};

class LocalTableStorage : public std::enable_shared_from_this<LocalTableStorage> {
public:
	// Create a new LocalTableStorage
	explicit LocalTableStorage(DataTable &table);
	// Create a LocalTableStorage from an ALTER TYPE
	LocalTableStorage(DataTable &table, LocalTableStorage &parent, idx_t changed_idx, const LogicalType &target_type,
	                  const vector<column_t> &bound_columns, Expression &cast_expr);
	// Create a LocalTableStorage from a DROP COLUMN
	LocalTableStorage(DataTable &table, LocalTableStorage &parent, idx_t drop_idx);
	// Create a LocalTableStorage from an ADD COLUMN
	LocalTableStorage(DataTable &table, LocalTableStorage &parent, ColumnDefinition &new_column,
	                  Expression *default_value);
	~LocalTableStorage();

	DataTable *table;

	Allocator &allocator;
	//! The main chunk collection holding the data
	shared_ptr<RowGroupCollection> row_groups;
	//! The set of unique indexes
	TableIndexList indexes;
	//! The number of deleted rows
	idx_t deleted_rows;
	//! The optimistic data writer
	OptimisticDataWriter optimistic_writer;

public:
	void InitializeScan(CollectionScanState &state, TableFilterSet *table_filters = nullptr);
	//! Check if we should flush the previously written row-group to disk
	void CheckFlushToDisk();
	//! Flushes the final row group to disk (if any)
	void FlushToDisk();
	void Rollback();
	idx_t EstimatedSize();

	void AppendToIndexes(Transaction &transaction, TableAppendState &append_state, idx_t append_count,
	                     bool append_to_table);
};

class LocalTableManager {
public:
	shared_ptr<LocalTableStorage> MoveEntry(DataTable *table);
	unordered_map<DataTable *, shared_ptr<LocalTableStorage>> MoveEntries();
	LocalTableStorage *GetStorage(DataTable *table);
	LocalTableStorage *GetOrCreateStorage(DataTable *table);
	idx_t EstimatedSize();
	bool IsEmpty();
	void InsertEntry(DataTable *table, shared_ptr<LocalTableStorage> entry);

private:
	mutex table_storage_lock;
	unordered_map<DataTable *, shared_ptr<LocalTableStorage>> table_storage;
};

//! The LocalStorage class holds appends that have not been committed yet
class LocalStorage {
public:
	// Threshold to merge row groups instead of appending
	static constexpr const idx_t MERGE_THRESHOLD = RowGroup::ROW_GROUP_SIZE / 2;

public:
	struct CommitState {
		unordered_map<DataTable *, unique_ptr<TableAppendState>> append_states;
	};

public:
	explicit LocalStorage(Transaction &transaction);

	static LocalStorage &Get(Transaction &transaction);
	static LocalStorage &Get(ClientContext &context);

	//! Initialize a scan of the local storage
	void InitializeScan(DataTable *table, CollectionScanState &state, TableFilterSet *table_filters);
	//! Scan
	void Scan(CollectionScanState &state, const vector<column_t> &column_ids, DataChunk &result);

	void InitializeParallelScan(DataTable *table, ParallelCollectionScanState &state);
	bool NextParallelScan(ClientContext &context, DataTable *table, ParallelCollectionScanState &state,
	                      CollectionScanState &scan_state);

	//! Begin appending to the local storage
	void InitializeAppend(LocalAppendState &state, DataTable *table);
	//! Append a chunk to the local storage
	static void Append(LocalAppendState &state, DataChunk &chunk);
	//! Finish appending to the local storage
	static void FinalizeAppend(LocalAppendState &state);
	//! Merge a row group collection into the transaction-local storage
	void LocalMerge(DataTable *table, RowGroupCollection &collection);

	//! Delete a set of rows from the local storage
	idx_t Delete(DataTable *table, Vector &row_ids, idx_t count);
	//! Update a set of rows in the local storage
	void Update(DataTable *table, Vector &row_ids, const vector<column_t> &column_ids, DataChunk &data);

	//! Commits the local storage, writing it to the WAL and completing the commit
	void Commit(LocalStorage::CommitState &commit_state, Transaction &transaction);
	//! Rollback the local storage
	void Rollback();

	bool ChangesMade() noexcept;
	idx_t EstimatedSize();

	bool Find(DataTable *table);

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
	Transaction &transaction;
	LocalTableManager table_manager;

	void Flush(DataTable &table, LocalTableStorage &storage);
};

} // namespace duckdb
