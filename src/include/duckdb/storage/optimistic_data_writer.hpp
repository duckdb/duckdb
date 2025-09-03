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
	OptimisticDataWriter(ClientContext &context, DataTable &table);
	OptimisticDataWriter(DataTable &table, OptimisticDataWriter &parent);
	~OptimisticDataWriter();

	//! Write a new row group to disk (if possible)
	void WriteNewRowGroup(RowGroupCollection &row_groups);
	//! Write the last row group of a collection to disk
	void WriteLastRowGroup(RowGroupCollection &row_groups);
	//! Final flush of the optimistic writer - fully flushes the partial block manager
	void FinalFlush();
	//! Flushes a specific row group to disk
	void FlushToDisk(RowGroup &row_group);
	//! Merge the partially written blocks from one optimistic writer into another
	void Merge(OptimisticDataWriter &other);
	//! Rollback
	void Rollback();

	//! Return the client context.
	ClientContext &GetClientContext() {
		return context;
	}

private:
	//! Prepare a write to disk
	bool PrepareWrite();

private:
	//! The client context in which we're writing the data.
	ClientContext &context;
	//! The table.
	DataTable &table;
	//! The partial block manager, if any.
	unique_ptr<PartialBlockManager> partial_manager;
};

} // namespace duckdb
