//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_group.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_size.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/enums/scan_options.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/parser/column_list.hpp"
#include "duckdb/storage/table/segment_base.hpp"
#include "duckdb/storage/block.hpp"

namespace duckdb {
class AttachedDatabase;
class BlockManager;
class ColumnData;
class DatabaseInstance;
class DataTable;
class PartialBlockManager;
struct DataTableInfo;
class ExpressionExecutor;
class RowGroupCollection;
class RowGroupWriter;
class UpdateSegment;
class TableStatistics;
struct ColumnSegmentInfo;
class Vector;
struct ColumnCheckpointState;
struct RowGroupPointer;
struct TransactionData;
class CollectionScanState;
class TableFilterSet;
struct ColumnFetchState;
struct RowGroupAppendState;
class MetadataManager;
class RowVersionManager;

struct RowGroupWriteData {
	vector<unique_ptr<ColumnCheckpointState>> states;
	vector<BaseStatistics> statistics;
};

class RowGroup : public SegmentBase<RowGroup> {
public:
	friend class ColumnData;

public:
	RowGroup(RowGroupCollection &collection, idx_t start, idx_t count);
	RowGroup(RowGroupCollection &collection, RowGroupPointer &&pointer);
	~RowGroup();

private:
	//! The RowGroupCollection this row-group is a part of
	reference<RowGroupCollection> collection;
	//! The version info of the row_group (inserted and deleted tuple info)
	shared_ptr<RowVersionManager> version_info;
	//! The column data of the row_group
	vector<shared_ptr<ColumnData>> columns;

public:
	void MoveToCollection(RowGroupCollection &collection, idx_t new_start);
	RowGroupCollection &GetCollection() {
		return collection.get();
	}
	BlockManager &GetBlockManager();
	DataTableInfo &GetTableInfo();

	unique_ptr<RowGroup> AlterType(RowGroupCollection &collection, const LogicalType &target_type, idx_t changed_idx,
	                               ExpressionExecutor &executor, CollectionScanState &scan_state,
	                               DataChunk &scan_chunk);
	unique_ptr<RowGroup> AddColumn(RowGroupCollection &collection, ColumnDefinition &new_column,
	                               ExpressionExecutor &executor, Expression &default_value, Vector &intermediate);
	unique_ptr<RowGroup> RemoveColumn(RowGroupCollection &collection, idx_t removed_column);

	void CommitDrop();
	void CommitDropColumn(idx_t index);

	void InitializeEmpty(const vector<LogicalType> &types);

	//! Initialize a scan over this row_group
	bool InitializeScan(CollectionScanState &state);
	bool InitializeScanWithOffset(CollectionScanState &state, idx_t vector_offset);
	//! Checks the given set of table filters against the row-group statistics. Returns false if the entire row group
	//! can be skipped.
	bool CheckZonemap(TableFilterSet &filters, const vector<column_t> &column_ids);
	//! Checks the given set of table filters against the per-segment statistics. Returns false if any segments were
	//! skipped.
	bool CheckZonemapSegments(CollectionScanState &state);
	void Scan(TransactionData transaction, CollectionScanState &state, DataChunk &result);
	void ScanCommitted(CollectionScanState &state, DataChunk &result, TableScanType type);

	idx_t GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);
	idx_t GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
	                            SelectionVector &sel_vector, idx_t max_count);

	//! For a specific row, returns true if it should be used for the transaction and false otherwise.
	bool Fetch(TransactionData transaction, idx_t row);
	//! Fetch a specific row from the row_group and insert it into the result at the specified index
	void FetchRow(TransactionData transaction, ColumnFetchState &state, const vector<column_t> &column_ids,
	              row_t row_id, DataChunk &result, idx_t result_idx);

	//! Append count rows to the version info
	void AppendVersionInfo(TransactionData transaction, idx_t count);
	//! Commit a previous append made by RowGroup::AppendVersionInfo
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t count);
	//! Revert a previous append made by RowGroup::AppendVersionInfo
	void RevertAppend(idx_t start);

	//! Delete the given set of rows in the version manager
	idx_t Delete(TransactionData transaction, DataTable &table, row_t *row_ids, idx_t count);

	RowGroupWriteData WriteToDisk(PartialBlockManager &manager, const vector<CompressionType> &compression_types);
	bool AllDeleted();
	RowGroupPointer Checkpoint(RowGroupWriter &writer, TableStatistics &global_stats);

	void InitializeAppend(RowGroupAppendState &append_state);
	void Append(RowGroupAppendState &append_state, DataChunk &chunk, idx_t append_count);

	void Update(TransactionData transaction, DataChunk &updates, row_t *ids, idx_t offset, idx_t count,
	            const vector<PhysicalIndex> &column_ids);
	//! Update a single column; corresponds to DataTable::UpdateColumn
	//! This method should only be called from the WAL
	void UpdateColumn(TransactionData transaction, DataChunk &updates, Vector &row_ids,
	                  const vector<column_t> &column_path);

	void MergeStatistics(idx_t column_idx, const BaseStatistics &other);
	void MergeIntoStatistics(idx_t column_idx, BaseStatistics &other);
	unique_ptr<BaseStatistics> GetStatistics(idx_t column_idx);

	void GetColumnSegmentInfo(idx_t row_group_index, vector<ColumnSegmentInfo> &result);

	void Verify();

	void NextVector(CollectionScanState &state);

	idx_t DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count);
	RowVersionManager &GetOrCreateVersionInfo();

	// Serialization
	static void Serialize(RowGroupPointer &pointer, Serializer &serializer);
	static RowGroupPointer Deserialize(Deserializer &deserializer);

private:
	shared_ptr<RowVersionManager> &GetVersionInfo();
	shared_ptr<RowVersionManager> &GetOrCreateVersionInfoPtr();

	ColumnData &GetColumn(storage_t c);
	idx_t GetColumnCount() const;
	vector<shared_ptr<ColumnData>> &GetColumns();

	template <TableScanType TYPE>
	void TemplatedScan(TransactionData transaction, CollectionScanState &state, DataChunk &result);

	vector<MetaBlockPointer> CheckpointDeletes(MetadataManager &manager);

	bool HasUnloadedDeletes() const;

private:
	mutex row_group_lock;
	mutex stats_lock;
	vector<MetaBlockPointer> column_pointers;
	unique_ptr<atomic<bool>[]> is_loaded;
	vector<MetaBlockPointer> deletes_pointers;
	atomic<bool> deletes_is_loaded;
};

} // namespace duckdb
