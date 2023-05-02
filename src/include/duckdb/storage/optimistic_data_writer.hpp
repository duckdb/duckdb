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

	//! Write all row groups except the last to disk
	void WriteAllButLastRowGroup(RowGroupCollection &row_groups);
	//! Write a new row group to disk (if possible)
	void WriteNewRowGroup(RowGroupCollection &row_groups);
	//! Flushes a specific row group to disk
	void FlushToDisk(RowGroup *row_group);
	//! Merge the partially written blocks from one optimistic writer into another
	void Merge(OptimisticDataWriter &other);
	//! Rollback
	void Rollback();
	//! Clear partially written blocks without writing them to disk
	void ClearBlocks();

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
