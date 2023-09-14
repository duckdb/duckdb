//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/data_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/index_type.hpp"
#include "duckdb/common/enums/scan_options.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/table/table_statistics.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/statistics/column_statistics.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/storage/table/data_table_info.hpp"
#include "duckdb/common/unique_ptr.hpp"

namespace duckdb {
class BoundForeignKeyConstraint;
class ClientContext;
class ColumnDataCollection;
class ColumnDefinition;
class DataTable;
class DuckTransaction;
class OptimisticDataWriter;
class RowGroup;
class StorageManager;
class TableCatalogEntry;
class TableIOManager;
class Transaction;
class WriteAheadLog;
class TableDataWriter;
class ConflictManager;
class TableScanState;
enum class VerifyExistenceType : uint8_t;

//! DataTable represents a physical table on disk
class DataTable {
public:
	//! Constructs a new data table from an (optional) set of persistent segments
	DataTable(AttachedDatabase &db, shared_ptr<TableIOManager> table_io_manager, const string &schema,
	          const string &table, vector<ColumnDefinition> column_definitions_p,
	          unique_ptr<PersistentTableData> data = nullptr);
	//! Constructs a DataTable as a delta on an existing data table with a newly added column
	DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression &default_value);
	//! Constructs a DataTable as a delta on an existing data table but with one column removed
	DataTable(ClientContext &context, DataTable &parent, idx_t removed_column);
	//! Constructs a DataTable as a delta on an existing data table but with one column changed type
	DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
	          const vector<column_t> &bound_columns, Expression &cast_expr);
	//! Constructs a DataTable as a delta on an existing data table but with one column added new constraint
	explicit DataTable(ClientContext &context, DataTable &parent, unique_ptr<BoundConstraint> constraint);

	//! The table info
	shared_ptr<DataTableInfo> info;
	//! The set of physical columns stored by this DataTable
	vector<ColumnDefinition> column_definitions;
	//! A reference to the database instance
	AttachedDatabase &db;

public:
	//! Returns a list of types of the table
	vector<LogicalType> GetTypes();

	void InitializeScan(TableScanState &state, const vector<column_t> &column_ids,
	                    TableFilterSet *table_filter = nullptr);
	void InitializeScan(DuckTransaction &transaction, TableScanState &state, const vector<column_t> &column_ids,
	                    TableFilterSet *table_filters = nullptr);

	//! Returns the maximum amount of threads that should be assigned to scan this data table
	idx_t MaxThreads(ClientContext &context);
	void InitializeParallelScan(ClientContext &context, ParallelTableScanState &state);
	bool NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state);

	//! Scans up to STANDARD_VECTOR_SIZE elements from the table starting
	//! from offset and store them in result. Offset is incremented with how many
	//! elements were returned.
	//! Returns true if all pushed down filters were executed during data fetching
	void Scan(DuckTransaction &transaction, DataChunk &result, TableScanState &state);

	//! Fetch data from the specific row identifiers from the base table
	void Fetch(DuckTransaction &transaction, DataChunk &result, const vector<column_t> &column_ids,
	           const Vector &row_ids, idx_t fetch_count, ColumnFetchState &state);

	//! Initializes an append to transaction-local storage
	void InitializeLocalAppend(LocalAppendState &state, ClientContext &context);
	//! Append a DataChunk to the transaction-local storage of the table.
	void LocalAppend(LocalAppendState &state, TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
	                 bool unsafe = false);
	//! Finalizes a transaction-local append
	void FinalizeLocalAppend(LocalAppendState &state);
	//! Append a chunk to the transaction-local storage of this table
	void LocalAppend(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk);
	//! Append a column data collection to the transaction-local storage of this table
	void LocalAppend(TableCatalogEntry &table, ClientContext &context, ColumnDataCollection &collection);
	//! Merge a row group collection into the transaction-local storage
	void LocalMerge(ClientContext &context, RowGroupCollection &collection);
	//! Creates an optimistic writer for this table - used for optimistically writing parallel appends
	OptimisticDataWriter &CreateOptimisticWriter(ClientContext &context);
	void FinalizeOptimisticWriter(ClientContext &context, OptimisticDataWriter &writer);

	//! Delete the entries with the specified row identifier from the table
	idx_t Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_ids, idx_t count);
	//! Update the entries with the specified row identifier from the table
	void Update(TableCatalogEntry &table, ClientContext &context, Vector &row_ids,
	            const vector<PhysicalIndex> &column_ids, DataChunk &data);
	//! Update a single (sub-)column along a column path
	//! The column_path vector is a *path* towards a column within the table
	//! i.e. if we have a table with a single column S STRUCT(A INT, B INT)
	//! and we update the validity mask of "S.B"
	//! the column path is:
	//! 0 (first column of table)
	//! -> 1 (second subcolumn of struct)
	//! -> 0 (first subcolumn of INT)
	//! This method should only be used from the WAL replay. It does not verify update constraints.
	void UpdateColumn(TableCatalogEntry &table, ClientContext &context, Vector &row_ids,
	                  const vector<column_t> &column_path, DataChunk &updates);

	//! Add an index to the DataTable. NOTE: for CREATE (UNIQUE) INDEX statements, we use the PhysicalCreateARTIndex
	//! operator. This function is only used during the WAL replay, and is a much less performant index creation
	//! approach.
	void WALAddIndex(ClientContext &context, unique_ptr<Index> index,
	                 const vector<unique_ptr<Expression>> &expressions);

	//! Fetches an append lock
	void AppendLock(TableAppendState &state);
	//! Begin appending structs to this table, obtaining necessary locks, etc
	void InitializeAppend(DuckTransaction &transaction, TableAppendState &state, idx_t append_count);
	//! Append a chunk to the table using the AppendState obtained from InitializeAppend
	void Append(DataChunk &chunk, TableAppendState &state);
	//! Commit the append
	void CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count);
	//! Write a segment of the table to the WAL
	void WriteToLog(WriteAheadLog &log, idx_t row_start, idx_t count);
	//! Revert a set of appends made by the given AppendState, used to revert appends in the event of an error during
	//! commit (e.g. because of an I/O exception)
	void RevertAppend(idx_t start_row, idx_t count);
	void RevertAppendInternal(idx_t start_row, idx_t count);

	void ScanTableSegment(idx_t start_row, idx_t count, const std::function<void(DataChunk &chunk)> &function);

	//! Merge a row group collection directly into this table - appending it to the end of the table without copying
	void MergeStorage(RowGroupCollection &data, TableIndexList &indexes);

	//! Append a chunk with the row ids [row_start, ..., row_start + chunk.size()] to all indexes of the table, returns
	//! whether or not the append succeeded
	PreservedError AppendToIndexes(DataChunk &chunk, row_t row_start);
	static PreservedError AppendToIndexes(TableIndexList &indexes, DataChunk &chunk, row_t row_start);
	//! Remove a chunk with the row ids [row_start, ..., row_start + chunk.size()] from all indexes of the table
	void RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start);
	//! Remove the chunk with the specified set of row identifiers from all indexes of the table
	void RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, Vector &row_identifiers);
	//! Remove the row identifiers from all the indexes of the table
	void RemoveFromIndexes(Vector &row_identifiers, idx_t count);

	void SetAsRoot() {
		this->is_root = true;
	}
	bool IsRoot() {
		return this->is_root;
	}

	//! Get statistics of a physical column within the table
	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id);
	//! Sets statistics of a physical column within the table
	void SetDistinct(column_t column_id, unique_ptr<DistinctStatistics> distinct_stats);

	//! Checkpoint the table to the specified table data writer
	void Checkpoint(TableDataWriter &writer, Serializer &metadata_serializer);
	void CommitDropTable();
	void CommitDropColumn(idx_t index);

	idx_t GetTotalRows();

	vector<ColumnSegmentInfo> GetColumnSegmentInfo();
	static bool IsForeignKeyIndex(const vector<PhysicalIndex> &fk_keys, Index &index, ForeignKeyType fk_type);

	//! Initializes a special scan that is used to create an index on the table, it keeps locks on the table
	void InitializeWALCreateIndexScan(CreateIndexScanState &state, const vector<column_t> &column_ids);
	//! Scans the next chunk for the CREATE INDEX operator
	bool CreateIndexScan(TableScanState &state, DataChunk &result, TableScanType type);

	//! Verify constraints with a chunk from the Append containing all columns of the table
	void VerifyAppendConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk,
	                             ConflictManager *conflict_manager = nullptr);

public:
	static void VerifyUniqueIndexes(TableIndexList &indexes, ClientContext &context, DataChunk &chunk,
	                                ConflictManager *conflict_manager);

private:
	//! Verify the new added constraints against current persistent&local data
	void VerifyNewConstraint(ClientContext &context, DataTable &parent, const BoundConstraint *constraint);
	//! Verify constraints with a chunk from the Update containing only the specified column_ids
	void VerifyUpdateConstraints(ClientContext &context, TableCatalogEntry &table, DataChunk &chunk,
	                             const vector<PhysicalIndex> &column_ids);
	//! Verify constraints with a chunk from the Delete containing all columns of the table
	void VerifyDeleteConstraints(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk);

	void InitializeScanWithOffset(TableScanState &state, const vector<column_t> &column_ids, idx_t start_row,
	                              idx_t end_row);

	void VerifyForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context, DataChunk &chunk,
	                                VerifyExistenceType verify_type);
	void VerifyAppendForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
	                                      DataChunk &chunk);
	void VerifyDeleteForeignKeyConstraint(const BoundForeignKeyConstraint &bfk, ClientContext &context,
	                                      DataChunk &chunk);

private:
	//! Lock for appending entries to the table
	mutex append_lock;
	//! The row groups of the table
	shared_ptr<RowGroupCollection> row_groups;
	//! Whether or not the data table is the root DataTable for this table; the root DataTable is the newest version
	//! that can be appended to
	atomic<bool> is_root;
};
} // namespace duckdb
