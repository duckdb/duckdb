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

struct DeleteInfo;
class MetadataManager;
struct MetaBlockPointer;

class RowVersionManager {
public:
	explicit RowVersionManager(idx_t start);

	idx_t GetStart() {
		return start;
	}
	void SetStart(idx_t start);
	idx_t GetCommittedDeletedCount(idx_t count);

	idx_t GetSelVector(TransactionData transaction, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);
	idx_t GetCommittedSelVector(transaction_t start_time, transaction_t transaction_id, idx_t vector_idx,
	                            SelectionVector &sel_vector, idx_t max_count);
	bool Fetch(TransactionData transaction, idx_t row);

	void AppendVersionInfo(TransactionData transaction, idx_t count, idx_t row_group_start, idx_t row_group_end);
	void CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count);
	void RevertAppend(idx_t start_row);

	idx_t DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count);
	void CommitDelete(idx_t vector_idx, transaction_t commit_id, const DeleteInfo &info);

	vector<MetaBlockPointer> Checkpoint(MetadataManager &manager);
	static shared_ptr<RowVersionManager> Deserialize(MetaBlockPointer delete_pointer, MetadataManager &manager,
	                                                 idx_t start);

private:
	mutex version_lock;
	idx_t start;
	unique_ptr<ChunkInfo> vector_info[Storage::ROW_GROUP_VECTOR_COUNT];
	bool has_changes;
	vector<MetaBlockPointer> storage_pointers;

private:
	optional_ptr<ChunkInfo> GetChunkInfo(idx_t vector_idx);
	ChunkVectorInfo &GetVectorInfo(idx_t vector_idx);
};

} // namespace duckdb
