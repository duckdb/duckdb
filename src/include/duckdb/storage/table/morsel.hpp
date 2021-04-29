//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/morsel.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_size.hpp"
#include "duckdb/storage/table/segment_base.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/table/append_state.hpp"
#include "duckdb/storage/table/scan_state.hpp"
#include "duckdb/storage/statistics/segment_statistics.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {
class ColumnData;
class DatabaseInstance;
class DataTable;
struct DataTableInfo;
class ExpressionExecutor;
class UpdateSegment;
class Vector;
struct VersionNode;

class Morsel : public SegmentBase {
public:
	friend class VersionDeleteState;
public:
	static constexpr const idx_t MORSEL_VECTOR_COUNT = 100;
	static constexpr const idx_t MORSEL_SIZE = STANDARD_VECTOR_SIZE * MORSEL_VECTOR_COUNT;

	static constexpr const idx_t MORSEL_LAYER_COUNT = 10;
	static constexpr const idx_t MORSEL_LAYER_SIZE = MORSEL_SIZE / MORSEL_LAYER_COUNT;

public:
	Morsel(DatabaseInstance &db, DataTableInfo &table_info, idx_t start, idx_t count);
	~Morsel();

private:
	//! The database instance
	DatabaseInstance &db;
	//! The table info of this morsel
	DataTableInfo &table_info;
	//! The version info of the morsel (inserted and deleted tuple info)
	shared_ptr<VersionNode> version_info;
	//! The column data of the morsel
	vector<shared_ptr<ColumnData>> columns;
	//! The update data of the morsel
	vector<shared_ptr<UpdateSegment>> updates;
	//! The segment statistics for each of the columns
	vector<shared_ptr<SegmentStatistics>> stats;

public:
	DatabaseInstance &GetDatabase() {
		return db;
	}

	unique_ptr<Morsel> AlterType(ClientContext &context, const LogicalType &target_type, idx_t changed_idx, ExpressionExecutor &executor, TableScanState &scan_state, DataChunk &scan_chunk);
	unique_ptr<Morsel> AddColumn(ClientContext &context, ColumnDefinition &new_column, ExpressionExecutor &executor, Expression *default_value, Vector &intermediate);
	unique_ptr<Morsel> RemoveColumn(idx_t removed_column);

	void InitializeEmpty(const vector<LogicalType> &types);

	//! Initialize a scan over this morsel
	void InitializeScan(MorselScanState &state);
	void InitializeScanWithOffset(MorselScanState &state, idx_t vector_offset);
	void Scan(Transaction &transaction, MorselScanState &state, DataChunk &result);
	void IndexScan(MorselScanState &state, DataChunk &result, bool allow_pending_updates);

	idx_t GetSelVector(Transaction &transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);

	//! For a specific row, returns true if it should be used for the transaction and false otherwise.
	bool Fetch(Transaction &transaction, idx_t row);
	//! Fetch a specific row from the morsel and insert it into the result at the specified index
	void FetchRow(Transaction &transaction, ColumnFetchState &state, const vector<column_t> &column_ids, row_t row_id, DataChunk &result, idx_t result_idx);

	//! Append count rows to the version info
	void AppendVersionInfo(Transaction &transaction, idx_t start, idx_t count, transaction_t commit_id);
	//! Commit a previous append made by Morsel::AppendVersionInfo
	void CommitAppend(transaction_t commit_id, idx_t start, idx_t count);
	//! Revert a previous append made by Morsel::AppendVersionInfo
	void RevertAppend(idx_t start);

	//! Delete the given set of rows in the version manager
	void Delete(Transaction &transaction, DataTable *table, Vector &row_ids, idx_t count);

	void InitializeAppend(Transaction &transaction, MorselAppendState &append_state, idx_t remaining_append_count);
	void Append(MorselAppendState &append_state, DataChunk &chunk, idx_t append_count);

	void Update(Transaction &transaction, DataChunk &updates, Vector &row_ids, const vector<column_t> &column_ids);

	void MergeStatistics(idx_t column_idx, BaseStatistics &other);
	unique_ptr<BaseStatistics> GetStatistics(idx_t column_idx);
private:
	ChunkInfo *GetChunkInfo(idx_t vector_idx);

	template<bool SCAN_DELETES, bool SCAN_COMMITTED, bool ALLOW_UPDATES>
	void TemplatedScan(Transaction *transaction, MorselScanState &state, DataChunk &result);

private:
	mutex morsel_lock;
	mutex stats_lock;
};

struct VersionNode {
	unique_ptr<ChunkInfo> info[Morsel::MORSEL_VECTOR_COUNT];
};

} // namespace duckdb
