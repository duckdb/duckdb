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
class TableStorageInfo;
class Vector;
struct ColumnCheckpointState;
struct RowGroupPointer;
struct TransactionData;
struct VersionNode;
class CollectionScanState;
class TableFilterSet;
struct ColumnFetchState;
struct RowGroupAppendState;

struct RowGroupWriteData {
	vector<unique_ptr<ColumnCheckpointState>> states;
	vector<BaseStatistics> statistics;
};

class RowGroup : public SegmentBase<RowGroup> {
public:
	friend class ColumnData;
	friend class VersionDeleteState;

public:
	static constexpr const idx_t ROW_GROUP_SIZE = STANDARD_ROW_GROUPS_SIZE;
	static constexpr const idx_t ROW_GROUP_VECTOR_COUNT = ROW_GROUP_SIZE / STANDARD_VECTOR_SIZE;

public:
	RowGroup(RowGroupCollection &collection, idx_t start, idx_t count);
	RowGroup(RowGroupCollection &collection, RowGroupPointer &&pointer);
	RowGroup(RowGroup &row_group, RowGroupCollection &collection, idx_t start);
	~RowGroup();

private:
	//! The RowGroupCollection this row-group is a part of
	RowGroupCollection &collection;
	//! The version info of the row_group (inserted and deleted tuple info)
	shared_ptr<VersionNode> version_info;

public:
	RowGroupCollection &GetCollection() {
		return collection;
	}
	DatabaseInstance &GetDatabase();
	BlockManager &GetBlockManager();
	DataTableInfo &GetTableInfo();

	//! Initialize a scan over this row_group
	void InitializeScan(CollectionScanState &state);
	void InitializeScanWithOffset(CollectionScanState &state, idx_t vector_offset);

	idx_t GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);
	idx_t GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
	                            SelectionVector &sel_vector, idx_t max_count);

	//! For a specific row, returns true if it should be used for the transaction and false otherwise.
	bool Fetch(TransactionData transaction, idx_t row);

	//! Append count rows to the version info
	void AppendVersionInfo(TransactionData transaction, idx_t count);
	//! Commit a previous append made by RowGroup::AppendVersionInfo
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t count);
	//! Revert a previous append made by RowGroup::AppendVersionInfo
	void RevertAppend(idx_t start);

	//! Delete the given set of rows in the version manager
	idx_t Delete(TransactionData transaction, DataTable *table, row_t *row_ids, idx_t count);

	static void Serialize(RowGroupPointer &pointer, Serializer &serializer);
	static RowGroupPointer Deserialize(Deserializer &source, const vector<LogicalType> &columns);

	void InitializeAppend(RowGroupAppendState &append_state);
	void Append(RowGroupAppendState &append_state, DataChunk &chunk, idx_t append_count);

private:
	ChunkInfo *GetChunkInfo(idx_t vector_idx);

	static void CheckpointDeletes(VersionNode *versions, Serializer &serializer);
	static shared_ptr<VersionNode> DeserializeDeletes(Deserializer &source);

private:
	mutex row_group_lock;
};

struct VersionNode {
	unique_ptr<ChunkInfo> info[RowGroup::ROW_GROUP_VECTOR_COUNT];

	void SetStart(idx_t start);
};

} // namespace duckdb
