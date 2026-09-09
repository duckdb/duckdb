//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/table_statistics.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/reservoir_sample.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/storage/statistics/column_statistics.hpp"
#include "duckdb/storage/storage_index.hpp"

namespace duckdb {
class ColumnList;
class PersistentTableData;
class Serializer;
class Deserializer;

class DUCKDB_SCOPED_CAPABILITY TableStatisticsLock {
public:
	explicit TableStatisticsLock(annotated_mutex &l) DUCKDB_ACQUIRE(l) : guard(l), locked_mutex(l) {
	}

	~TableStatisticsLock() DUCKDB_RELEASE() = default;

	void AssertHeld(const annotated_mutex &expected) const DUCKDB_ASSERT_CAPABILITY(expected) {
		D_ASSERT(&locked_mutex == &expected);
	}

	lock_guard<mutex> guard;

private:
	annotated_mutex &locked_mutex;
};

class TableStatistics {
public:
	void Initialize(const vector<LogicalType> &types, PersistentTableData &data);
	void InitializeEmpty(const vector<LogicalType> &types);
	void InitializeEmpty(const TableStatistics &other);

	void InitializeAddColumn(TableStatistics &parent, const LogicalType &new_column_type);
	void InitializeRemoveColumn(TableStatistics &parent, idx_t removed_column);
	void InitializeAlterType(TableStatistics &parent, idx_t changed_idx, const LogicalType &new_type);
	void InitializeAddConstraint(TableStatistics &parent);

	void MergeStats(TableStatistics &other);
	void MergeStats(idx_t i, BaseStatistics &stats);
	void MergeStats(TableStatisticsLock &lock, idx_t i, BaseStatistics &stats,
	                StatsMergeType merge_type = StatsMergeType::MERGE_STATS);

	void SetStats(TableStatistics &other);
	void CopyStats(TableStatistics &other);
	void CopyStats(TableStatisticsLock &lock, TableStatistics &other);
	unique_ptr<BaseStatistics> CopyStats(const StorageIndex &i);
	//! Get a reference to the stats - this requires us to hold the lock.
	//! The reference can only be safely accessed while the lock is held
	ColumnStatistics &GetStats(TableStatisticsLock &lock, idx_t i);
	//! Get a reference to the table sample - this requires us to hold the lock.
	// BlockingSample &GetTableSampleRef(TableStatisticsLock &lock);
	//! Take ownership of the sample, needed for merging. Requires the lock
	unique_ptr<BlockingSample> GetTableSample(TableStatisticsLock &lock);
	void SetTableSample(TableStatisticsLock &lock, unique_ptr<BlockingSample> sample);

	void DestroyTableSample(TableStatisticsLock &lock) const;
	void AppendToTableSample(TableStatisticsLock &lock, unique_ptr<BlockingSample> sample);

	bool Empty() const;

	unique_ptr<TableStatisticsLock> GetLock();

	void Serialize(Serializer &serializer) const;
	void Deserialize(Deserializer &deserializer, ColumnList &columns);

private:
	//! Shared by table versions created by ALTER; held until the statistics lock token is destroyed.
	shared_ptr<annotated_mutex> stats_lock;
	//! Column statistics
	vector<shared_ptr<ColumnStatistics>> column_stats;
	//! The table sample
	unique_ptr<BlockingSample> table_sample;
};

} // namespace duckdb
