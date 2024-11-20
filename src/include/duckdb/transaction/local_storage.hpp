//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/local_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {
class AttachedDatabase;
class Catalog;
class DataTable;
class StorageCommitState;
class Transaction;
class WriteAheadLog;
struct LocalAppendState;
struct TableAppendState;

class LocalTableStorage : public enable_shared_from_this<LocalTableStorage> {
public:
	// Create a new LocalTableStorage
	explicit LocalTableStorage(ClientContext &context, DataTable &table);
	// Create a LocalTableStorage from an ALTER TYPE
	LocalTableStorage(ClientContext &context, DataTable &table, LocalTableStorage &parent, idx_t changed_idx,
	                  const LogicalType &target_type, const vector<StorageIndex> &bound_columns, Expression &cast_expr);
	// Create a LocalTableStorage from a DROP COLUMN
	LocalTableStorage(DataTable &table, LocalTableStorage &parent, idx_t drop_idx);
	// Create a LocalTableStorage from an ADD COLUMN
	LocalTableStorage(ClientContext &context, DataTable &table, LocalTableStorage &parent, ColumnDefinition &new_column,
	                  ExpressionExecutor &default_executor);
	~LocalTableStorage();

	reference<DataTable> table_ref;

	Allocator &allocator;
	//! The main chunk collection holding the data
	shared_ptr<RowGroupCollection> row_groups;
	//! The set of unique indexes
	TableIndexList indexes;
	//! The number of deleted rows
	idx_t deleted_rows;
	//! The main optimistic data writer
	OptimisticDataWriter optimistic_writer;
	//! The set of all optimistic data writers associated with this table
	vector<unique_ptr<OptimisticDataWriter>> optimistic_writers;
	//! Whether or not storage was merged
	bool merged_storage = false;
	//! Whether or not the storage was dropped
	bool is_dropped = false;

public:
	void InitializeScan(CollectionScanState &state, optional_ptr<TableFilterSet> table_filters = nullptr);
	//! Write a new row group to disk (if possible)
	void WriteNewRowGroup();
	void FlushBlocks();
	void Rollback();
	idx_t EstimatedSize();

	void AppendToIndexes(DuckTransaction &transaction, TableAppendState &append_state, idx_t append_count,
	                     bool append_to_table);
	ErrorData AppendToIndexes(DuckTransaction &transaction, RowGroupCollection &source, TableIndexList &index_list,
	                          const vector<LogicalType> &table_types, row_t &start_row);

	//! Creates an optimistic writer for this table
	OptimisticDataWriter &CreateOptimisticWriter();
	void FinalizeOptimisticWriter(OptimisticDataWriter &writer);
};

class LocalTableManager {
public:
	shared_ptr<LocalTableStorage> MoveEntry(DataTable &table);
	reference_map_t<DataTable, shared_ptr<LocalTableStorage>> MoveEntries();
	optional_ptr<LocalTableStorage> GetStorage(DataTable &table);
	LocalTableStorage &GetOrCreateStorage(ClientContext &context, DataTable &table);
	idx_t EstimatedSize();
	bool IsEmpty();
	void InsertEntry(DataTable &table, shared_ptr<LocalTableStorage> entry);

private:
	mutex table_storage_lock;
	reference_map_t<DataTable, shared_ptr<LocalTableStorage>> table_storage;
};

//! The LocalStorage class holds appends that have not been committed yet
class LocalStorage {
public:
	struct CommitState {
		CommitState();
		~CommitState();

		reference_map_t<DataTable, unique_ptr<TableAppendState>> append_states;
	};

public:
	explicit LocalStorage(ClientContext &context, DuckTransaction &transaction);

	static LocalStorage &Get(DuckTransaction &transaction);
	static LocalStorage &Get(ClientContext &context, AttachedDatabase &db);
	static LocalStorage &Get(ClientContext &context, Catalog &catalog);

	//! Initialize a scan of the local storage
	void InitializeScan(DataTable &table, CollectionScanState &state, optional_ptr<TableFilterSet> table_filters);
	//! Scan
	void Scan(CollectionScanState &state, const vector<StorageIndex> &column_ids, DataChunk &result);

	void InitializeParallelScan(DataTable &table, ParallelCollectionScanState &state);
	bool NextParallelScan(ClientContext &context, DataTable &table, ParallelCollectionScanState &state,
	                      CollectionScanState &scan_state);

	//! Begin appending to the local storage
	void InitializeAppend(LocalAppendState &state, DataTable &table);
	//! Append a chunk to the local storage
	static void Append(LocalAppendState &state, DataChunk &chunk);
	//! Finish appending to the local storage
	static void FinalizeAppend(LocalAppendState &state);
	//! Merge a row group collection into the transaction-local storage
	void LocalMerge(DataTable &table, RowGroupCollection &collection);
	//! Create an optimistic writer for the specified table
	OptimisticDataWriter &CreateOptimisticWriter(DataTable &table);
	void FinalizeOptimisticWriter(DataTable &table, OptimisticDataWriter &writer);

	//! Delete a set of rows from the local storage
	idx_t Delete(DataTable &table, Vector &row_ids, idx_t count);
	//! Update a set of rows in the local storage
	void Update(DataTable &table, Vector &row_ids, const vector<PhysicalIndex> &column_ids, DataChunk &data);

	//! Commits the local storage, writing it to the WAL and completing the commit
	void Commit(optional_ptr<StorageCommitState> commit_state);
	//! Rollback the local storage
	void Rollback();

	bool ChangesMade() noexcept;
	idx_t EstimatedSize();

	void DropTable(DataTable &table);
	bool Find(DataTable &table);

	idx_t AddedRows(DataTable &table);

	void AddColumn(DataTable &old_dt, DataTable &new_dt, ColumnDefinition &new_column,
	               ExpressionExecutor &default_executor);
	void DropColumn(DataTable &old_dt, DataTable &new_dt, idx_t removed_column);
	void ChangeType(DataTable &old_dt, DataTable &new_dt, idx_t changed_idx, const LogicalType &target_type,
	                const vector<StorageIndex> &bound_columns, Expression &cast_expr);

	void MoveStorage(DataTable &old_dt, DataTable &new_dt);
	void FetchChunk(DataTable &table, Vector &row_ids, idx_t count, const vector<StorageIndex> &col_ids,
	                DataChunk &chunk, ColumnFetchState &fetch_state);
	TableIndexList &GetIndexes(DataTable &table);

	void VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint);

private:
	ClientContext &context;
	DuckTransaction &transaction;
	LocalTableManager table_manager;

	void Flush(DataTable &table, LocalTableStorage &storage, optional_ptr<StorageCommitState> commit_state);
};

} // namespace duckdb
