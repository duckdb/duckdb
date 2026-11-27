//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/transaction/local_storage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/table_index_list.hpp"
#include "duckdb/storage/optimistic_data_writer.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/main/context.hpp"
#include "duckdb/common/reference_map.hpp"

namespace duckdb {
class AttachedDatabase;
class Allocator;
class BoundConstraint;
class Catalog;
class CollectionScanState;
class ColumnDefinition;
class DataChunk;
class DataTable;
class DuckTableEntry;
class DuckTransaction;
class Expression;
class ExpressionExecutor;
class RowGroupCollection;
class StorageCommitState;
class Transaction;
class Vector;
class WriteAheadLog;
struct ColumnFetchState;
struct LocalAppendState;
struct ParallelCollectionScanState;
struct TableAppendState;
struct TransactionData;

//! The transaction-local indexes of a table: shared by the storages of the versions of the table (see ALTER)
struct LocalStorageIndexes {
	//! The set of unique append indexes.
	TableIndexList append_indexes;
	//! The set of delete indexes.
	TableIndexList delete_indexes;
};

class LocalTableStorage : public enable_shared_from_this<LocalTableStorage> {
public:
	// Create a new LocalTableStorage
	explicit LocalTableStorage(Context &context, DataTable &table);
	//! Create the LocalTableStorage of the new version of a table from an ALTER TYPE: the storage of the old version
	//! is not modified (it can still be in use), the new one shares what can be shared with it.
	LocalTableStorage(ClientContext &context, DataTable &new_data_table, LocalTableStorage &parent,
	                  const idx_t alter_column_index, const LogicalType &target_type,
	                  const vector<StorageIndex> &bound_columns, Expression &cast_expr, TransactionData transaction);
	//! Create the LocalTableStorage of the new version of a table from a DROP COLUMN.
	LocalTableStorage(ClientContext &context, DataTable &new_data_table, LocalTableStorage &parent,
	                  const idx_t drop_column_index);
	//! Create the LocalTableStorage of the new version of a table from an ADD CONSTRAINT (the row groups are shared)
	LocalTableStorage(ClientContext &context, DataTable &new_data_table, LocalTableStorage &parent);
	//! Create the LocalTableStorage of the new version of a table from an ADD COLUMN
	LocalTableStorage(ClientContext &context, DataTable &table, LocalTableStorage &parent, ColumnDefinition &new_column,
	                  ExpressionExecutor &default_executor);
	~LocalTableStorage();

	reference<DataTable> table_ref;
	//! The DuckTableEntry visible to this transaction that references table_ref.
	//! Updated through the append path (InitializeAppend, Append, LocalMerge) and
	//! propagated from parent storage on ALTER operations. Atomic: connections that share a transaction set it
	//! concurrently.
	atomic<optional_ptr<DuckTableEntry>> table_entry;

	Allocator &allocator;
	//! The main row group collection.
	unique_ptr<OptimisticWriteCollection> row_groups;
	//! Set to INSERT_DUPLICATES, if we are skipping constraint checking during, e.g., WAL replay.
	IndexAppendMode index_append_mode = IndexAppendMode::DEFAULT;
	//! The number of deleted rows (parallel DELETEs update it concurrently)
	atomic<idx_t> deleted_rows;

	//! The optimistic row group collections associated with this table.
	vector<unique_ptr<OptimisticWriteCollection>> optimistic_collections;
	//! The main optimistic data writer associated with this table.
	OptimisticDataWriter optimistic_writer;

	//! Whether or not the storage was dropped
	atomic<bool> is_dropped {false};
	//! Whether an ALTER of a connection sharing the transaction replaced the storage by the one of the new table
	atomic<bool> replaced_by_alter {false};

	//! Serializes the appends and merges into the primary collection
	StorageLock append_lock;
	//! Shared by the users of the primary collection, exclusive while its row groups are replaced by a flush
	StorageLock flush_lock;

public:
	//! Locks to use the primary collection (throw if the table was altered in the meantime)
	unique_ptr<StorageLockKey> LockShared();
	unique_ptr<StorageLockKey> LockAppend();
	//! Initialize a scan; the context of the scanning query is used for pushed down casts and zonemap checks
	void InitializeScan(Context &context, CollectionScanState &state,
	                    optional_ptr<TableFilterSet> table_filters = nullptr);
	//! Write a new row group to disk (if possible)
	void WriteNewRowGroup(Context &context, idx_t flushed_row_group_idx);
	void FlushBlocks(Context &context);
	//! Whether Flush() takes the bulk-append path for this storage: the append covers at least one
	//! full row group and there are no deletes. Only depends on transaction-local state, i.e. this
	//! can be decided before taking any locks.
	bool IsBulkAppend() const;
	//! Whether the optimistic writer of this storage writes to disk (not temporary / in-memory / read-only)
	bool WritesToDisk() const;
	//! Whether this storage holds optimistically written (flushed) row groups
	bool HasFlushedRowGroups() const;
	void Rollback();
	idx_t EstimatedSize() const;

	//! Appends to the table's indexes at commit. A returned error means the indexes were restored.
	ErrorData AppendToIndexes(DuckTransaction &transaction, TableAppendState &append_state);
	//! Appends to the table at commit. A returned error means the table and its indexes were restored.
	ErrorData AppendToTable(DuckTransaction &transaction, TableAppendState &append_state);
	ErrorData AppendToIndexes(DuckTransaction &transaction, RowGroupCollection &source, TableIndexList &index_list,
	                          const vector<LogicalType> &table_types, row_t &start_row);
	//! Removes the rows of this storage that were appended to the indexes of the table at [row_start, row_end)
	ErrorData RevertAppendToIndexes(DuckTransaction &transaction, row_t row_start, row_t row_end);
	void AppendToDeleteIndexes(Vector &row_ids, DataChunk &delete_chunk);

	//! Create an optimistic row group collection for this table.
	//! Returns the index into the optimistic_collections vector for newly created collection.
	PhysicalIndex CreateOptimisticCollection(unique_ptr<OptimisticWriteCollection> collection);
	//! Returns the optimistic row group collection corresponding to the index.
	OptimisticWriteCollection &GetOptimisticCollection(const PhysicalIndex collection_index);
	//! Resets the optimistic row group collection corresponding to the index.
	void ResetOptimisticCollection(const PhysicalIndex collection_index);
	//! Whether a statement is writing an optimistic row group collection of the table
	bool HasOptimisticCollections();
	//! Returns the optimistic writer.
	OptimisticDataWriter &GetOptimisticWriter();

	RowGroupCollection &GetCollection();
	OptimisticWriteCollection &GetPrimaryCollection();
	TableIndexList &GetAppendIndexes() const {
		return indexes->append_indexes;
	}
	TableIndexList &GetDeleteIndexes() const {
		return indexes->delete_indexes;
	}

private:
	//! The transaction-local indexes, shared with the storage of the next version of the table after an ALTER
	shared_ptr<LocalStorageIndexes> indexes;
	mutex collections_lock;
};

class LocalTableManager {
public:
	shared_ptr<LocalTableStorage> MoveEntry(DataTable &table);
	//! Replaces the storage of a table by the storage of its altered version
	void ReplaceEntry(DataTable &old_table, DataTable &new_table, shared_ptr<LocalTableStorage> entry);
	reference_map_t<const DataTable, shared_ptr<LocalTableStorage>> MoveEntries();
	vector<shared_ptr<LocalTableStorage>> GetEntries() const;
	shared_ptr<LocalTableStorage> GetStorage(const DataTable &table) const;
	shared_ptr<LocalTableStorage> GetOrCreateStorage(Context &context, DataTable &table);
	idx_t EstimatedSize() const;
	bool IsEmpty() const;
	void InsertEntry(DataTable &table, shared_ptr<LocalTableStorage> entry);

private:
	mutable mutex table_storage_lock;
	reference_map_t<const DataTable, shared_ptr<LocalTableStorage>> table_storage;
	//! The old versions of the tables whose storage was replaced by an ALTER
	reference_set_t<const DataTable> altered_tables;
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
	explicit LocalStorage(DuckTransaction &transaction);

	static LocalStorage &Get(DuckTransaction &transaction);
	static LocalStorage &Get(ClientContext &context, AttachedDatabase &db);
	static LocalStorage &Get(ClientContext &context, Catalog &catalog);

	//! Initialize a scan of the local storage
	void InitializeScan(Context &context, DataTable &table, CollectionScanState &state,
	                    optional_ptr<TableFilterSet> table_filters);
	//! Scan
	void Scan(CollectionScanState &state, const vector<StorageIndex> &column_ids, DataChunk &result);

	void InitializeParallelScan(DataTable &table, ParallelCollectionScanState &state);
	optional_idx NextParallelScan(ClientContext &context, DataTable &table, ParallelCollectionScanState &state,
	                              CollectionScanState &scan_state, bool initialize_columns = true);

	//! Begin appending to the local storage
	void InitializeAppend(Context &context, LocalAppendState &state, DataTable &table, DuckTableEntry &table_entry);
	//! Initialize the storage and its indexes, but no row groups.
	void InitializeStorage(Context &context, LocalAppendState &state, DataTable &table, DuckTableEntry &table_entry);

	//! Append a chunk to the local storage
	static void Append(LocalAppendState &state, DuckTableEntry &table_entry, DataChunk &table_chunk);
	//! Finish appending to the local storage
	static void FinalizeAppend(LocalAppendState &state);
	//! Merge a row group collection into the transaction-local storage
	void LocalMerge(Context &context, DataTable &table, DuckTableEntry &table_entry,
	                OptimisticWriteCollection &collection);
	//! Create an optimistic row group collection for this table.
	//! Returns the index into the optimistic_collections vector for newly created collection.
	PhysicalIndex CreateOptimisticCollection(Context &context, DataTable &table,
	                                         unique_ptr<OptimisticWriteCollection> collection);
	//! Returns the optimistic row group collection corresponding to the index.
	OptimisticWriteCollection &GetOptimisticCollection(Context &context, DataTable &table,
	                                                   const PhysicalIndex collection_index);
	//! Resets the optimistic row group collection corresponding to the index.
	void ResetOptimisticCollection(Context &context, DataTable &table, const PhysicalIndex collection_index);
	//! Returns the optimistic writer.
	OptimisticDataWriter &GetOptimisticWriter(Context &context, DataTable &table);

	//! Delete a set of rows from the local storage
	idx_t Delete(Context &context, DataTable &table, DuckTableEntry &table_entry, Vector &row_ids, idx_t count);
	//! Update a set of rows in the local storage
	void Update(DataTable &table, DuckTableEntry &table_entry, Vector &row_ids, const vector<PhysicalIndex> &column_ids,
	            DataChunk &data);

	//! Commits the local storage, writing it to the WAL and completing the commit
	void Commit(Context &context, optional_ptr<StorageCommitState> commit_state);
	//! Rollback the local storage
	void Rollback();

	bool ChangesMade() noexcept;
	idx_t EstimatedSize();

	void DropTable(DataTable &table);
	bool Find(DataTable &table);

	idx_t AddedRows(DataTable &table);
	vector<PartitionStatistics> GetPartitionStats(DataTable &table, TransactionData transaction) const;

	void AddColumn(ClientContext &context, DataTable &old_dt, DataTable &new_dt, ColumnDefinition &new_column,
	               ExpressionExecutor &default_executor);
	void DropColumn(ClientContext &context, DataTable &old_dt, DataTable &new_dt, const idx_t drop_column_index);
	void ChangeType(ClientContext &context, DataTable &old_dt, DataTable &new_dt, idx_t changed_idx,
	                const LogicalType &target_type, const vector<StorageIndex> &bound_columns, Expression &cast_expr);

	void MoveStorage(ClientContext &context, DataTable &old_dt, DataTable &new_dt);
	void FetchChunk(DataTable &table, const Vector &row_ids, idx_t count, const vector<StorageIndex> &col_ids,
	                DataChunk &chunk, ColumnFetchState &fetch_state);
	//! Returns true, if the local storage contains the row id.
	bool CanFetch(DataTable &table, const row_t row_id);
	TableIndexList &GetIndexes(Context &context, DataTable &table);
	shared_ptr<LocalTableStorage> GetStorage(const DataTable &table);

	void VerifyNewConstraint(Context &context, DataTable &parent, const BoundConstraint &constraint);

	DuckTransaction &GetTransaction() const {
		return transaction;
	}

	void FlushBulkAppendBlocksAndSync(Context &context, AttachedDatabase &db);
	bool SyncedFlushedBlocks() const {
		return synced_flushed_blocks;
	}

private:
	//! The locks that are held while the storage of an altered table is replaced
	struct LocalStorageAlterLocks {
		unique_ptr<StorageLockKey> append_key;
		unique_ptr<StorageLockKey> flush_key;
	};

	DuckTransaction &transaction;
	LocalTableManager table_manager;
	bool synced_flushed_blocks = false;

private:
	//! Takes the locks of the storage of the table that an ALTER replaces. Throws if other statements use it
	shared_ptr<LocalTableStorage> BeginAlter(DataTable &old_dt, LocalStorageAlterLocks &locks);
	//! Replaces the storage of the old version of the table by the one of the new version
	void FinishAlter(DataTable &old_dt, DataTable &new_dt, shared_ptr<LocalTableStorage> new_storage);
	//! Flushes the local rows into the shared table and indexes; invalidates the database if they cannot be restored
	void Flush(Context &context, DataTable &table, LocalTableStorage &storage,
	           optional_ptr<StorageCommitState> commit_state);
};

} // namespace duckdb
