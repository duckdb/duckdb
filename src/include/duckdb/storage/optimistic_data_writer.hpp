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
	OptimisticDataWriter(DataTable &table, shared_ptr<PartialBlockManager> partial_manager);
	OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent);
	~OptimisticDataWriter();

	void CheckFlushToDisk(RowGroupCollection &row_groups);
	//! Flushes a specific row group to disk
	void FlushToDisk(RowGroup *row_group);
	//! Flushes the final row group to disk (if any)
	void FlushToDisk(RowGroupCollection &row_groups, bool force = false);

	void Rollback();

private:
	//! Prepare a write to disk
	bool PrepareWrite();

private:
	//! The table
	DataTable &table;
	//! The partial block manager
	shared_ptr<PartialBlockManager> partial_manager;
	//! The set of blocks that have been pre-emptively written to disk
	unordered_set<block_id_t> written_blocks;
	//! Whether or not anything has been written so far
	bool written_anything;
};

} // namespace duckdb
