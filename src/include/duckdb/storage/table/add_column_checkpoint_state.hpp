//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/add_column_checkpoint_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/partial_block_manager.hpp"

namespace duckdb {

class AddColumnCheckpointState {
public:
	AddColumnCheckpointState(ClientContext &context, DataTable &table, AddColumnInfo &info);
	virtual ~AddColumnCheckpointState();

	//! Flush all row groups to disk
	void FlushToDisk(RowGroupCollection &data);

	//! Get `stable_result` to replay it
	shared_ptr<PersistentCollectionData> GetStableResult();

private:
	//! Prepare a write to disk
	bool PrepareWrite();

private:
	ClientContext &context;
	DataTable &table;
	AddColumnInfo &info;
	unique_ptr<PartialBlockManager> partial_manager;
	vector<CompressionType> compression_types;
};

} // namespace duckdb
