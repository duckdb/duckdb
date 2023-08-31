//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_version_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/vector_size.hpp"
#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/storage/storage_info.hpp"
#include "duckdb/common/mutex.hpp"

namespace duckdb {

class RowVersionManager {
	friend class VersionDeleteState;
public:
	void SetStart(idx_t start);
	idx_t GetCommittedDeletedCount(idx_t count);

	idx_t GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);
	idx_t GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
										  SelectionVector &sel_vector, idx_t max_count);
	bool Fetch(TransactionData transaction, idx_t row);

	void AppendVersionInfo(TransactionData transaction, idx_t count, idx_t row_group_start, idx_t row_group_end, idx_t start);
	void CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count);
	void RevertAppend(idx_t start_row);

	MetaBlockPointer Checkpoint(MetadataManager &manager);
	static shared_ptr<RowVersionManager> Deserialize(MetaBlockPointer delete_pointer, MetadataManager &manager);

private:
	mutex version_lock;
	unique_ptr<ChunkInfo> vector_info[Storage::ROW_GROUP_VECTOR_COUNT];

private:
	optional_ptr<ChunkInfo> GetChunkInfo(idx_t vector_idx);
};

} // namespace duckdb
