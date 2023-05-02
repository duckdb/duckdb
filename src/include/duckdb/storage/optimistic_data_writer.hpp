//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/optimistic_data_writer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/row_group_collection.hpp"

namespace duckdb {
class PartialBlockManager;

class OptimisticDataWriter {
public:
	OptimisticDataWriter(DataTable &table);
	OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent);
	~OptimisticDataWriter();

	void CheckFlushToDisk(RowGroupCollection &row_groups);
	//! Flushes a specific row group to disk
	void FlushToDisk(RowGroup *row_group);
	//! Flushes the final row group to disk (if any)
	void FlushToDisk(RowGroupCollection &row_groups, bool force);
	//! Final flush: flush the partial block manager to disk
	void FinalFlush();
	//! Merge the partially written blocks from one optimistic writer into another
	void Merge(OptimisticDataWriter &other);
	//! Rollback
	void Rollback();

private:
	//! Prepare a write to disk
	bool PrepareWrite();

private:
	//! The table
	DataTable &table;
	//! The partial block manager (if we created one yet)
	unique_ptr<PartialBlockManager> partial_manager;
};

} // namespace duckdb
