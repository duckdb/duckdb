//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/table/row_version_manager.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/storage/table/chunk_info.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"

namespace duckdb {

struct DeleteInfo;
class MetadataManager;
class BufferManager;
struct MetaBlockPointer;

class RowVersionManager {
public:
	explicit RowVersionManager(BufferManager &buffer_manager) noexcept;

	idx_t GetCommittedDeletedCount(idx_t count);

	bool ShouldCheckpointRowGroup(transaction_t checkpoint_id, idx_t count);
	idx_t GetSelVector(ScanOptions options, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);
	bool Fetch(TransactionData transaction, idx_t row);

	void AppendVersionInfo(TransactionData transaction, idx_t count, idx_t row_group_start, idx_t row_group_end);
	void CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count);
	void RevertAppend(idx_t new_count);
	void CleanupAppend(transaction_t lowest_active_transaction, idx_t row_group_start, idx_t count);

	idx_t DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count);
	void CommitDelete(idx_t vector_idx, transaction_t commit_id, const DeleteInfo &info);

	vector<MetaBlockPointer> Checkpoint(RowGroupWriter &writer);
	static shared_ptr<RowVersionManager> Deserialize(MetaBlockPointer delete_pointer, MetadataManager &manager);

	bool HasUnserializedChanges();
	vector<MetaBlockPointer> GetStoragePointers();

private:
	mutex version_lock;
	FixedSizeAllocator allocator;
	vector<unique_ptr<ChunkInfo>> vector_info;
	optional_idx uncheckpointed_delete_commit;
	vector<MetaBlockPointer> storage_pointers;

private:
	FixedSizeAllocator &GetAllocator() {
		return allocator;
	}
	optional_ptr<ChunkInfo> GetChunkInfo(idx_t vector_idx);
	ChunkVectorInfo &GetVectorInfo(idx_t vector_idx);
	void FillVectorInfo(idx_t vector_idx);
};

} // namespace duckdb
