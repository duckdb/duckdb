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
	//! Create a LocalTableStorage from an ALTER TYPE.
	LocalTableStorage(ClientContext &context, DataTable &new_data_table, LocalTableStorage &parent,
	                  const idx_t alter_column_index, const LogicalType &target_type,
	                  const vector<StorageIndex> &bound_columns, Expression &cast_expr);
	//! Create a LocalTableStorage from a DROP COLUMN.
	LocalTableStorage(DataTable &new_data_table, LocalTableStorage &parent, const idx_t drop_column_index);
	// Create a LocalTableStorage from an ADD COLUMN
	LocalTableStorage(ClientContext &context, DataTable &table, LocalTableStorage &parent, ColumnDefinition &new_column,
	                  ExpressionExecutor &default_executor);
	~LocalTableStorage();

	reference<DataTable> table_ref;

	Allocator &allocator;
	//! The main row group collection.
	shared_ptr<RowGroupCollection> row_groups;
	//! The set of unique append indexes.
	TableIndexList append_indexes;
	//! The set of delete indexes.
	TableIndexList delete_indexes;
	//! Set to INSERT_DUPLICATES, if we are skipping constraint checking during, e.g., WAL replay.
	IndexAppendMode index_append_mode = IndexAppendMode::DEFAULT;
	//! The number of deleted rows
	idx_t deleted_rows;

	//! The optimistic row group collections associated with this table.
	vector<unique_ptr<RowGroupCollection>> optimistic_collections;
	//! The main optimistic data writer associated with this table.
	OptimisticDataWriter optimistic_writer;

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

	void AppendToIndexes(DuckTransaction &transaction, TableAppendState &append_state, bool append_to_table);
	ErrorData AppendToIndexes(DuckTransaction &transaction, RowGroupCollection &source, TableIndexList &index_list,
	                          const vector<LogicalType> &table_types, row_t &start_row);
	void AppendToDeleteIndexes(Vector &row_ids, DataChunk &delete_chunk);

	//! Create an optimistic row group collection for this table.
	//! Returns the index into the optimistic_collections vector for newly created collection.
	PhysicalIndex CreateOptimisticCollection(unique_ptr<RowGroupCollection> collection);
	//! Returns the optimistic row group collection corresponding to the index.
	RowGroupCollection &GetOptimisticCollection(const PhysicalIndex collection_index);
	//! Resets the optimistic row group collection corresponding to the index.
	void ResetOptimisticCollection(const PhysicalIndex collection_index);
	//! Returns the optimistic writer.
	OptimisticDataWriter &GetOptimisticWriter();

private:
	mutex collections_lock;
};

class LocalTableManager {
public:
	shared_ptr<LocalTableStorage> MoveEntry(DataTable &table);
	reference_map_t<DataTable, shared_ptr<LocalTableStorage>> MoveEntries();
	optional_ptr<LocalTableStorage> GetStorage(DataTable &table) const;
	LocalTableStorage &GetOrCreateStorage(ClientContext &context, DataTable &table);
	idx_t EstimatedSize() const;
	bool IsEmpty() const;
	void InsertEntry(DataTable &table, shared_ptr<LocalTableStorage> entry);

private:
	mutable mutex table_storage_lock;
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
	//! Initialize the storage and its indexes, but no row groups.
	void InitializeStorage(LocalAppendState &state, DataTable &table);
	//! Append a chunk to the local storage
	static void Append(LocalAppendState &state, DataChunk &chunk);
	//! Finish appending to the local storage
	static void FinalizeAppend(LocalAppendState &state);
	//! Merge a row group collection into the transaction-local storage
	void LocalMerge(DataTable &table, RowGroupCollection &collection);
	//! Create an optimistic row group collection for this table.
	//! Returns the index into the optimistic_collections vector for newly created collection.
	PhysicalIndex CreateOptimisticCollection(DataTable &table, unique_ptr<RowGroupCollection> collection);
	//! Returns the optimistic row group collection corresponding to the index.
	RowGroupCollection &GetOptimisticCollection(DataTable &table, const PhysicalIndex collection_index);
	//! Resets the optimistic row group collection corresponding to the index.
	void ResetOptimisticCollection(DataTable &table, const PhysicalIndex collection_index);
	//! Returns the optimistic writer.
	OptimisticDataWriter &GetOptimisticWriter(DataTable &table);

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
	vector<PartitionStatistics> GetPartitionStats(DataTable &table) const;

	void AddColumn(DataTable &old_dt, DataTable &new_dt, ColumnDefinition &new_column,
	               ExpressionExecutor &default_executor);
	void DropColumn(DataTable &old_dt, DataTable &new_dt, const idx_t drop_column_index);
	void ChangeType(DataTable &old_dt, DataTable &new_dt, idx_t changed_idx, const LogicalType &target_type,
	                const vector<StorageIndex> &bound_columns, Expression &cast_expr);

	void MoveStorage(DataTable &old_dt, DataTable &new_dt);
	void FetchChunk(DataTable &table, Vector &row_ids, idx_t count, const vector<StorageIndex> &col_ids,
	                DataChunk &chunk, ColumnFetchState &fetch_state);
	TableIndexList &GetIndexes(DataTable &table);
	optional_ptr<LocalTableStorage> GetStorage(DataTable &table);

	void VerifyNewConstraint(DataTable &parent, const BoundConstraint &constraint);

private:
	ClientContext &context;
	DuckTransaction &transaction;
	LocalTableManager table_manager;

private:
	void Flush(DataTable &table, LocalTableStorage &storage, optional_ptr<StorageCommitState> commit_state);
};

} // namespace duckdb
