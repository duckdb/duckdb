//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/data_table.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/index_type.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/storage/index.hpp"
#include "duckdb/storage/table_statistics.hpp"
#include "duckdb/storage/block.hpp"
#include "duckdb/storage/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table/persistent_segment.hpp"
#include "duckdb/transaction/local_storage.hpp"
#include "duckdb/storage/table/persistent_table_data.hpp"

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class ClientContext;
class ColumnDefinition;
class DataTable;
class StorageManager;
class TableCatalogEntry;
class Transaction;
class WriteAheadLog;
class TableDataWriter;

class TableIndexList {
public:
	//! Scan the catalog set, invoking the callback method for every entry
	template <class T>
	void Scan(T &&callback) {
		// lock the catalog set
		lock_guard<mutex> lock(indexes_lock);
		for (auto &index : indexes) {
			if (callback(*index)) {
				break;
			}
		}
	}

	void AddIndex(unique_ptr<Index> index) {
		D_ASSERT(index);
		lock_guard<mutex> lock(indexes_lock);
		indexes.push_back(move(index));
	}

	void RemoveIndex(Index *index) {
		D_ASSERT(index);
		lock_guard<mutex> lock(indexes_lock);

		for (idx_t index_idx = 0; index_idx < indexes.size(); index_idx++) {
			auto &index_entry = indexes[index_idx];
			if (index_entry.get() == index) {
				indexes.erase(indexes.begin() + index_idx);
				break;
			}
		}
	}

	bool Empty() {
		lock_guard<mutex> lock(indexes_lock);
		return indexes.empty();
	}

private:
	//! Indexes associated with the current table
	mutex indexes_lock;
	vector<unique_ptr<Index>> indexes;
};

struct DataTableInfo {
	DataTableInfo(string schema, string table) : cardinality(0), schema(move(schema)), table(move(table)) {
	}

	//! The amount of elements in the table. Note that this number signifies the amount of COMMITTED entries in the
	//! table. It can be inaccurate inside of transactions. More work is needed to properly support that.
	atomic<idx_t> cardinality;
	// schema of the table
	string schema;
	// name of the table
	string table;

	TableIndexList indexes;

	bool IsTemporary() {
		return schema == TEMP_SCHEMA;
	}
};

struct ParallelTableScanState {
	idx_t current_row;
	bool transaction_local_data;
};

//! DataTable represents a physical table on disk
class DataTable {
public:
	//! Constructs a new data table from an (optional) set of persistent segments
	DataTable(DatabaseInstance &db, const string &schema, const string &table, vector<LogicalType> types,
	          unique_ptr<PersistentTableData> data = nullptr);
	//! Constructs a DataTable as a delta on an existing data table with a newly added column
	DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression *default_value);
	//! Constructs a DataTable as a delta on an existing data table but with one column removed
	DataTable(ClientContext &context, DataTable &parent, idx_t removed_column);
	//! Constructs a DataTable as a delta on an existing data table but with one column changed type
	DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, const LogicalType &target_type,
	          vector<column_t> bound_columns, Expression &cast_expr);

	shared_ptr<DataTableInfo> info;
	//! Types managed by data table
	vector<LogicalType> types;
	//! A reference to the database instance
	DatabaseInstance &db;

public:
	void InitializeScan(TableScanState &state, const vector<column_t> &column_ids,
	                    TableFilterSet *table_filter = nullptr);
	void InitializeScan(Transaction &transaction, TableScanState &state, const vector<column_t> &column_ids,
	                    TableFilterSet *table_filters = nullptr);

	//! Returns the maximum amount of threads that should be assigned to scan this data table
	idx_t MaxThreads(ClientContext &context);
	void InitializeParallelScan(ParallelTableScanState &state);
	bool NextParallelScan(ClientContext &context, ParallelTableScanState &state, TableScanState &scan_state,
	                      const vector<column_t> &column_ids);

	//! Scans up to STANDARD_VECTOR_SIZE elements from the table starting
	//! from offset and store them in result. Offset is incremented with how many
	//! elements were returned.
	//! Returns true if all pushed down filters were executed during data fetching
	void Scan(Transaction &transaction, DataChunk &result, TableScanState &state, vector<column_t> &column_ids);

	//! Fetch data from the specific row identifiers from the base table
	void Fetch(Transaction &transaction, DataChunk &result, const vector<column_t> &column_ids, Vector &row_ids,
	           idx_t fetch_count, ColumnFetchState &state);

	//! Append a DataChunk to the table. Throws an exception if the columns don't match the tables' columns.
	void Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk);
	//! Delete the entries with the specified row identifier from the table
	void Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_ids, idx_t count);
	//! Update the entries with the specified row identifier from the table
	void Update(TableCatalogEntry &table, ClientContext &context, Vector &row_ids, const vector<column_t> &column_ids,
	            DataChunk &data);

	//! Add an index to the DataTable
	void AddIndex(unique_ptr<Index> index, const vector<unique_ptr<Expression>> &expressions);

	//! Begin appending structs to this table, obtaining necessary locks, etc
	void InitializeAppend(Transaction &transaction, TableAppendState &state, idx_t append_count);
	//! Append a chunk to the table using the AppendState obtained from BeginAppend
	void Append(Transaction &transaction, DataChunk &chunk, TableAppendState &state);
	//! Commit the append
	void CommitAppend(transaction_t commit_id, idx_t row_start, idx_t count);
	//! Write a segment of the table to the WAL
	void WriteToLog(WriteAheadLog &log, idx_t row_start, idx_t count);
	//! Revert a set of appends made by the given AppendState, used to revert appends in the event of an error during
	//! commit (e.g. because of an I/O exception)
	void RevertAppend(idx_t start_row, idx_t count);
	void RevertAppendInternal(idx_t start_row, idx_t count);

	void ScanTableSegment(idx_t start_row, idx_t count, const std::function<void(DataChunk &chunk)> &function);

	//! Append a chunk with the row ids [row_start, ..., row_start + chunk.size()] to all indexes of the table, returns
	//! whether or not the append succeeded
	bool AppendToIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start);
	//! Remove a chunk with the row ids [row_start, ..., row_start + chunk.size()] from all indexes of the table
	void RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, row_t row_start);
	//! Remove the chunk with the specified set of row identifiers from all indexes of the table
	void RemoveFromIndexes(TableAppendState &state, DataChunk &chunk, Vector &row_identifiers);
	//! Remove the row identifiers from all the indexes of the table
	void RemoveFromIndexes(Vector &row_identifiers, idx_t count);

	void SetAsRoot() {
		this->is_root = true;
	}

	unique_ptr<BaseStatistics> GetStatistics(ClientContext &context, column_t column_id);

	//! Checkpoint the table to the specified table data writer
	void Checkpoint(TableDataWriter &writer);
	void CheckpointDeletes(TableDataWriter &writer);
	void CommitDropTable();
	void CommitDropColumn(idx_t index);

	idx_t GetTotalRows();

private:
	//! Verify constraints with a chunk from the Append containing all columns of the table
	void VerifyAppendConstraints(TableCatalogEntry &table, DataChunk &chunk);
	//! Verify constraints with a chunk from the Update containing only the specified column_ids
	void VerifyUpdateConstraints(TableCatalogEntry &table, DataChunk &chunk, const vector<column_t> &column_ids);

	void InitializeScanWithOffset(TableScanState &state, const vector<column_t> &column_ids,
	                              TableFilterSet *table_filters, idx_t start_row, idx_t end_row);
	bool CheckZonemap(TableScanState &state, const vector<column_t> &column_ids, TableFilterSet *table_filters,
	                  idx_t &current_row);
	bool ScanBaseTable(Transaction &transaction, DataChunk &result, TableScanState &state,
	                   const vector<column_t> &column_ids, idx_t &current_row, idx_t max_row);
	bool ScanCreateIndex(CreateIndexScanState &state, const vector<column_t> &column_ids, DataChunk &result,
	                     idx_t &current_row, idx_t max_row, bool allow_pending_updates = false);

	//! Figure out which of the row ids to use for the given transaction by looking at inserted/deleted data. Returns
	//! the amount of rows to use and places the row_ids in the result_rows array.
	idx_t FetchRows(Transaction &transaction, Vector &row_identifiers, idx_t fetch_count, row_t result_rows[]);

	//! The CreateIndexScan is a special scan that is used to create an index on the table, it keeps locks on the table
	void InitializeCreateIndexScan(CreateIndexScanState &state, const vector<column_t> &column_ids);
	void CreateIndexScan(CreateIndexScanState &structure, const vector<column_t> &column_ids, DataChunk &result,
	                     bool allow_pending_updates = false);

private:
	//! Lock for appending entries to the table
	mutex append_lock;
	//! The segment tree holding the persistent versions
	shared_ptr<SegmentTree> versions;
	//! The number of rows in the table
	atomic<idx_t> total_rows;
	//! The physical columns of the table
	vector<shared_ptr<ColumnData>> columns;
	//! Whether or not the data table is the root DataTable for this table; the root DataTable is the newest version
	//! that can be appended to
	atomic<bool> is_root;
};
} // namespace duckdb
