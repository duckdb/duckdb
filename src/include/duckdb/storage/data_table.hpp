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
#include "duckdb/storage/table/version_manager.hpp"
#include "duckdb/transaction/local_storage.hpp"

#include <atomic>
#include <mutex>
#include <vector>

namespace duckdb {
class ClientContext;
class ColumnDefinition;
class DataTable;
class StorageManager;
class TableCatalogEntry;
class Transaction;

typedef unique_ptr<vector<unique_ptr<PersistentSegment>>[]> persistent_data_t;

//! TableFilter represents a filter pushed down into the table scan.
class TableFilter {
public:
	TableFilter(Value constant, ExpressionType comparison_type, idx_t column_index)
	    : constant(constant), comparison_type(comparison_type), column_index(column_index){};
	Value constant;
	ExpressionType comparison_type;
	idx_t column_index;
};

struct DataTableInfo {
	DataTableInfo(string schema, string table) : cardinality(0), schema(move(schema)), table(move(table)) {
	}

	//! The amount of elements in the table. Note that this number signifies the amount of COMMITTED entries in the
	//! table. It can be inaccurate inside of transactions. More work is needed to properly support that.
	std::atomic<idx_t> cardinality;
	// schema of the table
	string schema;
	// name of the table
	string table;
	//! Indexes associated with the current table
	vector<unique_ptr<Index>> indexes;

	bool IsTemporary() {
		return schema == TEMP_SCHEMA;
	}
};

//! DataTable represents a physical table on disk
class DataTable {
public:
	//! Constructs a new data table from an (optional) set of persistent segments
	DataTable(StorageManager &storage, string schema, string table, vector<TypeId> types, persistent_data_t data);
	//! Constructs a DataTable as a delta on an existing data table with a newly added column
	DataTable(ClientContext &context, DataTable &parent, ColumnDefinition &new_column, Expression *default_value);
	//! Constructs a DataTable as a delta on an existing data table but with one column removed
	DataTable(ClientContext &context, DataTable &parent, idx_t removed_column);
	//! Constructs a DataTable as a delta on an existing data table but with one column changed type
	DataTable(ClientContext &context, DataTable &parent, idx_t changed_idx, SQLType target_type,
	          vector<column_t> bound_columns, Expression &cast_expr);

	shared_ptr<DataTableInfo> info;
	//! Types managed by data table
	vector<TypeId> types;
	//! A reference to the base storage manager
	StorageManager &storage;

public:
	void InitializeScan(TableScanState &state, vector<column_t> column_ids,
	                    unordered_map<idx_t, vector<TableFilter>> *table_filter = nullptr);
	void InitializeScan(Transaction &transaction, TableScanState &state, vector<column_t> column_ids,
	                    unordered_map<idx_t, vector<TableFilter>> *table_filters = nullptr);
	//! Scans up to STANDARD_VECTOR_SIZE elements from the table starting
	//! from offset and store them in result. Offset is incremented with how many
	//! elements were returned.
	//! Returns true if all pushed down filters were executed during data fetching
	void Scan(Transaction &transaction, DataChunk &result, TableScanState &state,
	          unordered_map<idx_t, vector<TableFilter>> &table_filters);

	//! Initialize an index scan with a single predicate and a comparison type (= <= < > >=)
	void InitializeIndexScan(Transaction &transaction, TableIndexScanState &state, Index &index, Value value,
	                         ExpressionType expr_type, vector<column_t> column_ids);
	//! Initialize an index scan with two predicates and two comparison types (> >= < <=)
	void InitializeIndexScan(Transaction &transaction, TableIndexScanState &state, Index &index, Value low_value,
	                         ExpressionType low_type, Value high_value, ExpressionType high_type,
	                         vector<column_t> column_ids);
	//! Scans up to STANDARD_VECTOR_SIZE elements from the table from the given index structure
	void IndexScan(Transaction &transaction, DataChunk &result, TableIndexScanState &state);

	//! Fetch data from the specific row identifiers from the base table
	void Fetch(Transaction &transaction, DataChunk &result, vector<column_t> &column_ids, Vector &row_ids,
	           idx_t fetch_count, TableIndexScanState &state);

	//! Append a DataChunk to the table. Throws an exception if the columns don't match the tables' columns.
	void Append(TableCatalogEntry &table, ClientContext &context, DataChunk &chunk);
	//! Delete the entries with the specified row identifier from the table
	void Delete(TableCatalogEntry &table, ClientContext &context, Vector &row_ids, idx_t count);
	//! Update the entries with the specified row identifier from the table
	void Update(TableCatalogEntry &table, ClientContext &context, Vector &row_ids, vector<column_t> &column_ids,
	            DataChunk &data);

	//! Add an index to the DataTable
	void AddIndex(unique_ptr<Index> index, vector<unique_ptr<Expression>> &expressions);

	//! Begin appending structs to this table, obtaining necessary locks, etc
	void InitializeAppend(TableAppendState &state);
	//! Append a chunk to the table using the AppendState obtained from BeginAppend
	void Append(Transaction &transaction, transaction_t commit_id, DataChunk &chunk, TableAppendState &state);
	//! Revert a set of appends made by the given AppendState, used to revert appends in the event of an error during
	//! commit (e.g. because of an I/O exception)
	void RevertAppend(TableAppendState &state);

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

private:
	//! Verify constraints with a chunk from the Append containing all columns of the table
	void VerifyAppendConstraints(TableCatalogEntry &table, DataChunk &chunk);
	//! Verify constraints with a chunk from the Update containing only the specified column_ids
	void VerifyUpdateConstraints(TableCatalogEntry &table, DataChunk &chunk, vector<column_t> &column_ids);

	void InitializeIndexScan(Transaction &transaction, TableIndexScanState &state, Index &index,
	                         vector<column_t> column_ids);

	bool CheckZonemap(TableScanState &state, unordered_map<idx_t, vector<TableFilter>> &table_filters,
	                  idx_t &current_row);
	bool ScanBaseTable(Transaction &transaction, DataChunk &result, TableScanState &state, idx_t &current_row,
	                   idx_t max_row, idx_t base_row, VersionManager &manager,
	                   unordered_map<idx_t, vector<TableFilter>> &table_filters);
	bool ScanCreateIndex(CreateIndexScanState &state, DataChunk &result, idx_t &current_row, idx_t max_row,
	                     idx_t base_row);

	//! Figure out which of the row ids to use for the given transaction by looking at inserted/deleted data. Returns
	//! the amount of rows to use and places the row_ids in the result_rows array.
	idx_t FetchRows(Transaction &transaction, Vector &row_identifiers, idx_t fetch_count, row_t result_rows[]);

	//! The CreateIndexScan is a special scan that is used to create an index on the table, it keeps locks on the table
	void InitializeCreateIndexScan(CreateIndexScanState &state, vector<column_t> column_ids);
	void CreateIndexScan(CreateIndexScanState &structure, DataChunk &result);

private:
	//! Lock for appending entries to the table
	std::mutex append_lock;
	//! The version manager of the persistent segments of the tree
	shared_ptr<VersionManager> persistent_manager;
	//! The version manager of the transient segments of the tree
	shared_ptr<VersionManager> transient_manager;
	//! The physical columns of the table
	vector<shared_ptr<ColumnData>> columns;
	//! Whether or not the data table is the root DataTable for this table; the root DataTable is the newest version
	//! that can be appended to
	bool is_root;
};
} // namespace duckdb
