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
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/execution/index/fixed_size_allocator.hpp"
#include "duckdb/storage/checkpoint/row_group_writer.hpp"

namespace duckdb {

struct DeleteInfo;
class MetadataManager;
class BufferManager;
struct MetaBlockPointer;

class RowVersionManager {
public:
	explicit RowVersionManager(BufferManager &buffer_manager) noexcept;

	//! Returns the number of non-deleted rows in this segment
	idx_t GetRowCount(ScanOptions options, idx_t count);
	//! Count visible rows starting at a vector boundary, including a partial final vector
	idx_t GetRowCount(ScanOptions options, idx_t start_vector, idx_t count);

	idx_t GetSelVector(ScanOptions options, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count);
	//! Bulk visibility check. Returns the number of visible rows.
	idx_t GetVisibleRows(TransactionData transaction, const idx_t *offsets, idx_t count, SelectionVector &visible_sel);

	void AppendVersionInfo(TransactionData transaction, idx_t count, idx_t row_group_start, idx_t row_group_end);
	void CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count);
	void RevertAppend(idx_t new_count);
	void CleanupAppend(VisibilityBound lowest_visibility_bound, idx_t row_group_start, idx_t count);

	idx_t DeleteRows(idx_t vector_idx, TransactionData transaction, row_t rows[], idx_t count);
	void CommitDelete(idx_t vector_idx, transaction_t commit_id, const DeleteInfo &info);
	//! Whether the transaction marked rows of this row group as updated (and has an undo entry for them)
	bool HasUpdatedRows(transaction_t transaction_id);
	//! Marks the rows (ids[i] - offset) of the vector as updated by the transaction (see ChunkVectorInfo::Update)
	void UpdateRows(idx_t vector_idx, TransactionData transaction, const row_t ids[], idx_t count, row_t offset,
	                VisibilityBound lowest_visibility_bound);
	//! Replaces the id old_id of an update writer by new_id: a commit (transaction id to commit id), the revert of a
	//! commit, or a rollback (to 0, which leaves the previous writers of the rows)
	void ReplaceUpdateWriter(transaction_t old_id, transaction_t new_id);

	//! Attempts to compress the per-row insert/delete ids of each vector into constants. Ids that precede
	//! lowest_visibility_bound look the same to every active and future transaction, so they can collapse.
	//! Cheap when nothing can have changed: the pass only runs when version ids were modified since the
	//! last pass, or when a previous pass left ids that can still compress once older transactions finish
	void CompressVersionIds(VisibilityBound lowest_visibility_bound);

	vector<MetaBlockPointer> Checkpoint(RowGroupWriter &writer);
	static shared_ptr<RowVersionManager> Deserialize(MetaBlockPointer delete_pointer, MetadataManager &manager);

	bool HasUnserializedChanges(VisibilityBound bound);
	bool HasDeletes();
	bool HasUncommittedChanges();
	vector<MetaBlockPointer> GetStoragePointers();

private:
	mutex version_lock;
	FixedSizeAllocator allocator;
	vector<unique_ptr<ChunkVectorInfo>> vector_info;
	//! The transactions that updated rows of this row group, indexed by the writers of the rows (see
	//! RowUpdateWriters): a transaction id, its commit id once committed, or 0 once rolled back. Index 0 is no writer
	vector<transaction_t> update_writers;
	//! The writer index of each transaction that updates rows of this row group, until it commits or rolls back
	unordered_map<transaction_t, uint32_t> transaction_writers;
	//! The number of update writers at which a new writer first compacts them
	idx_t compact_update_writers_at = 64;
	//! The newest and the oldest delete commit that has not been written by a checkpoint yet
	optional_idx newest_uncheckpointed_delete_commit;
	optional_idx oldest_uncheckpointed_delete_commit;
	vector<MetaBlockPointer> storage_pointers;
	//! Whether a compression pass may achieve anything: set when version ids are modified, cleared when a
	//! pass finds no ids that could still compress. For deserialized version info this is derived from the
	//! deserialized content (with the current storage format checkpointed ids are always settled).
	bool needs_compression_check = false;

private:
	FixedSizeAllocator &GetAllocator() {
		return allocator;
	}
	optional_ptr<ChunkVectorInfo> GetChunkInfo(idx_t vector_idx);
	ChunkVectorInfo &GetVectorInfo(idx_t vector_idx);
	//! Forgets the update writers that all transactions see, or that rolled back
	void CompactUpdateWriters(VisibilityBound lowest_visibility_bound);
	void FillVectorInfo(idx_t vector_idx);
};

} // namespace duckdb
