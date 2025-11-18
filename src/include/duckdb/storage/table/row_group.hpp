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
#include "duckdb/common/enums/checkpoint_type.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/function/partition_stats.hpp"

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
struct PersistentColumnData;
struct PersistentRowGroupData;
struct RowGroupPointer;
struct TransactionData;
class CollectionScanState;
class TableFilter;
class TableFilterSet;
struct ColumnFetchState;
struct RowGroupAppendState;
class MetadataManager;
class RowVersionManager;
class ScanFilterInfo;
class StorageCommitState;
template <class T>
struct SegmentNode;
enum class ColumnDataType;

struct RowGroupWriteInfo {
	RowGroupWriteInfo(PartialBlockManager &manager, const vector<CompressionType> &compression_types,
	                  CheckpointType checkpoint_type = CheckpointType::FULL_CHECKPOINT);
	RowGroupWriteInfo(PartialBlockManager &manager, const vector<CompressionType> &compression_types,
	                  vector<unique_ptr<PartialBlockManager>> &column_partial_block_managers_p);

private:
	PartialBlockManager &manager;

public:
	const vector<CompressionType> &compression_types;
	CheckpointType checkpoint_type;

public:
	PartialBlockManager &GetPartialBlockManager(idx_t column_idx);

private:
	optional_ptr<vector<unique_ptr<PartialBlockManager>>> column_partial_block_managers;
};

struct RowGroupWriteData {
	shared_ptr<RowGroup> result_row_group;
	vector<unique_ptr<ColumnCheckpointState>> states;
	vector<BaseStatistics> statistics;
	bool reuse_existing_metadata_blocks = false;
	vector<idx_t> existing_extra_metadata_blocks;
};

class RowGroup : public SegmentBase<RowGroup> {
public:
	friend class ColumnData;

public:
	RowGroup(RowGroupCollection &collection, idx_t count);
	RowGroup(RowGroupCollection &collection, RowGroupPointer pointer);
	RowGroup(RowGroupCollection &collection, PersistentRowGroupData &data);
	~RowGroup();

private:
	//! The RowGroupCollection this row-group is a part of
	reference<RowGroupCollection> collection;
	//! The version info of the row_group (inserted and deleted tuple info)
	atomic<optional_ptr<RowVersionManager>> version_info;
	//! The owned version info of the row_group (inserted and deleted tuple info)
	shared_ptr<RowVersionManager> owned_version_info;
	//! The column data of the row_group (mutable because `const` can lazily load)
	mutable vector<shared_ptr<ColumnData>> columns;

public:
	void MoveToCollection(RowGroupCollection &collection);
	RowGroupCollection &GetCollection() const {
		return collection.get();
	}
	//! Returns the list of meta block pointers used by the columns
	vector<idx_t> GetOrComputeExtraMetadataBlocks(bool force_compute = false);

	const vector<MetaBlockPointer> &GetColumnStartPointers() const;

	//! Returns the list of meta block pointers used by the deletes
	const vector<MetaBlockPointer> &GetDeletesPointers() const {
		return deletes_pointers;
	}
	BlockManager &GetBlockManager() const;
	DataTableInfo &GetTableInfo() const;

	unique_ptr<RowGroup> AlterType(RowGroupCollection &collection, const LogicalType &target_type, idx_t changed_idx,
	                               ExpressionExecutor &executor, CollectionScanState &scan_state,
	                               SegmentNode<RowGroup> &node, DataChunk &scan_chunk);
	unique_ptr<RowGroup> AddColumn(RowGroupCollection &collection, ColumnDefinition &new_column,
	                               ExpressionExecutor &executor, Vector &intermediate);
	unique_ptr<RowGroup> RemoveColumn(RowGroupCollection &collection, idx_t removed_column);

	void CommitDrop();
	void CommitDropColumn(const idx_t column_index);

	void InitializeEmpty(const vector<LogicalType> &types, ColumnDataType data_type);
	bool HasChanges() const;

	//! Initialize a scan over this row_group
	bool InitializeScan(CollectionScanState &state, SegmentNode<RowGroup> &node);
	bool InitializeScanWithOffset(CollectionScanState &state, SegmentNode<RowGroup> &node, idx_t vector_offset);
	//! Checks the given set of table filters against the row-group statistics. Returns false if the entire row group
	//! can be skipped.
	bool CheckZonemap(ScanFilterInfo &filters);
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
	void FetchRow(TransactionData transaction, ColumnFetchState &state, const vector<StorageIndex> &column_ids,
	              row_t row_id, DataChunk &result, idx_t result_idx);

	//! Append count rows to the version info
	void AppendVersionInfo(TransactionData transaction, idx_t count);
	//! Commit a previous append made by RowGroup::AppendVersionInfo
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t count);
	//! Revert a previous append made by RowGroup::AppendVersionInfo
	void RevertAppend(idx_t new_count);
	//! Clean up append states that can either be compressed or deleted
	void CleanupAppend(transaction_t lowest_transaction, idx_t start, idx_t count);

	//! Delete the given set of rows in the version manager
	idx_t Delete(TransactionData transaction, DataTable &table, row_t *row_ids, idx_t count, idx_t row_group_start);

	static vector<RowGroupWriteData> WriteToDisk(RowGroupWriteInfo &info,
	                                             const vector<const_reference<RowGroup>> &row_groups);
	//! Write the data inside this RowGroup to disk and return a struct with information about the write
	//! Including the new RowGroup that contains the columns in their written-to-disk form
	RowGroupWriteData WriteToDisk(RowGroupWriteInfo &info) const;
	//! Returns the number of committed rows (count - committed deletes)
	idx_t GetCommittedRowCount();
	RowGroupWriteData WriteToDisk(RowGroupWriter &writer);
	RowGroupPointer Checkpoint(RowGroupWriteData write_data, RowGroupWriter &writer, TableStatistics &global_stats,
	                           idx_t row_group_start);
	bool IsPersistent() const;
	PersistentRowGroupData SerializeRowGroupInfo(idx_t row_group_start) const;

	void InitializeAppend(RowGroupAppendState &append_state);
	void Append(RowGroupAppendState &append_state, DataChunk &chunk, idx_t append_count);

	void Update(TransactionData transaction, DataTable &data_table, DataChunk &updates, row_t *ids, idx_t offset,
	            idx_t count, const vector<PhysicalIndex> &column_ids, idx_t row_group_start);
	//! Update a single column; corresponds to DataTable::UpdateColumn
	//! This method should only be called from the WAL
	void UpdateColumn(TransactionData transaction, DataTable &data_table, DataChunk &updates, Vector &row_ids,
	                  idx_t offset, idx_t count, const vector<column_t> &column_path, idx_t row_group_start);

	void MergeStatistics(idx_t column_idx, const BaseStatistics &other);
	void MergeIntoStatistics(idx_t column_idx, BaseStatistics &other);
	void MergeIntoStatistics(TableStatistics &other);
	unique_ptr<BaseStatistics> GetStatistics(idx_t column_idx);

	void GetColumnSegmentInfo(const QueryContext &context, idx_t row_group_index, vector<ColumnSegmentInfo> &result);
	PartitionStatistics GetPartitionStats(idx_t row_group_start) const;

	idx_t GetAllocationSize() const {
		return allocation_size;
	}

	void Verify();

	void NextVector(CollectionScanState &state);

	idx_t DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count);
	RowVersionManager &GetOrCreateVersionInfo();

	// Serialization
	static void Serialize(RowGroupPointer &pointer, Serializer &serializer);
	static RowGroupPointer Deserialize(Deserializer &deserializer);

	idx_t GetRowGroupSize() const;

	static FilterPropagateResult CheckRowIdFilter(const TableFilter &filter, idx_t beg_row, idx_t end_row);

private:
	optional_ptr<RowVersionManager> GetVersionInfo();
	shared_ptr<RowVersionManager> GetOrCreateVersionInfoPtr();
	shared_ptr<RowVersionManager> GetOrCreateVersionInfoInternal();
	void SetVersionInfo(shared_ptr<RowVersionManager> version);

	ColumnData &GetColumn(storage_t c) const;
	void LoadColumn(storage_t c) const;
	ColumnData &GetColumn(const StorageIndex &c) const;
	idx_t GetColumnCount() const;
	vector<shared_ptr<ColumnData>> &GetColumns();
	void LoadRowIdColumnData() const;
	void SetCount(idx_t count);

	template <TableScanType TYPE>
	void TemplatedScan(TransactionData transaction, CollectionScanState &state, DataChunk &result);

	vector<MetaBlockPointer> CheckpointDeletes(MetadataManager &manager);

	bool HasUnloadedDeletes() const;

private:
	mutable mutex row_group_lock;
	vector<MetaBlockPointer> column_pointers;
	//! Whether or not each column is loaded (mutable because `const` can lazy load)
	mutable unique_ptr<atomic<bool>[]> is_loaded;
	vector<MetaBlockPointer> deletes_pointers;
	bool has_metadata_blocks = false;
	vector<idx_t> extra_metadata_blocks;
	atomic<bool> deletes_is_loaded;
	atomic<idx_t> allocation_size;
	//! The row id column data (mutable because `const` can lazy load)
	mutable unique_ptr<ColumnData> row_id_column_data;
	//! Whether or not `row_id_column_data` is loaded (mutable because `const` can lazy load)
	mutable atomic<bool> row_id_is_loaded;
	atomic<bool> has_changes;
};

} // namespace duckdb
