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
	idx_t GetRowCount(ScanOptions options, idx_t count) DUCKDB_EXCLUDES(version_lock);

	idx_t GetSelVector(ScanOptions options, idx_t vector_idx, SelectionVector &sel_vector, idx_t max_count)
	    DUCKDB_EXCLUDES(version_lock);
	//! Bulk visibility check. Returns the number of visible rows.
	idx_t GetVisibleRows(TransactionData transaction, const idx_t *offsets, idx_t count, SelectionVector &visible_sel)
	    DUCKDB_EXCLUDES(version_lock);

	void AppendVersionInfo(TransactionData transaction, idx_t count, idx_t row_group_start, idx_t row_group_end)
	    DUCKDB_EXCLUDES(version_lock);
	void CommitAppend(transaction_t commit_id, idx_t row_group_start, idx_t count) DUCKDB_EXCLUDES(version_lock);
	void RevertAppend(idx_t new_count) DUCKDB_EXCLUDES(version_lock);
	void CleanupAppend(VisibilityBound lowest_visibility_bound, idx_t row_group_start, idx_t count)
	    DUCKDB_EXCLUDES(version_lock);

	idx_t DeleteRows(idx_t vector_idx, transaction_t transaction_id, row_t rows[], idx_t count)
	    DUCKDB_EXCLUDES(version_lock);
	void CommitDelete(idx_t vector_idx, transaction_t commit_id, const DeleteInfo &info) DUCKDB_EXCLUDES(version_lock);

	//! Attempts to compress the per-row insert/delete ids of each vector into constants. Ids that precede
	//! lowest_visibility_bound look the same to every active and future transaction, so they can collapse.
	//! Cheap when nothing can have changed: the pass only runs when version ids were modified since the
	//! last pass, or when a previous pass left ids that can still compress once older transactions finish
	void CompressVersionIds(VisibilityBound lowest_visibility_bound) DUCKDB_EXCLUDES(version_lock);

	vector<MetaBlockPointer> Checkpoint(RowGroupWriter &writer) DUCKDB_EXCLUDES(version_lock);
	static shared_ptr<RowVersionManager> Deserialize(MetaBlockPointer delete_pointer, MetadataManager &manager);

	bool HasUnserializedChanges() DUCKDB_EXCLUDES(version_lock);
	bool HasDeletes() DUCKDB_EXCLUDES(version_lock);
	bool HasUncommittedChanges() DUCKDB_EXCLUDES(version_lock);
	vector<MetaBlockPointer> GetStoragePointers() DUCKDB_EXCLUDES(version_lock);

private:
	annotated_mutex version_lock;
	FixedSizeAllocator allocator DUCKDB_GUARDED_BY(version_lock);
	vector<unique_ptr<ChunkVectorInfo>> vector_info DUCKDB_GUARDED_BY(version_lock);
	optional_idx uncheckpointed_delete_commit DUCKDB_GUARDED_BY(version_lock);
	vector<MetaBlockPointer> storage_pointers DUCKDB_GUARDED_BY(version_lock);
	//! Whether a compression pass may achieve anything: set when version ids are modified, cleared when a
	//! pass finds no ids that could still compress. For deserialized version info this is derived from the
	//! deserialized content (with the current storage format checkpointed ids are always settled).
	bool needs_compression_check DUCKDB_GUARDED_BY(version_lock) = false;

private:
	FixedSizeAllocator &GetAllocator() DUCKDB_REQUIRES(version_lock) {
		return allocator;
	}
	optional_ptr<ChunkVectorInfo> GetChunkInfo(idx_t vector_idx) DUCKDB_REQUIRES(version_lock);
	ChunkVectorInfo &GetVectorInfo(idx_t vector_idx) DUCKDB_REQUIRES(version_lock);
	void FillVectorInfo(idx_t vector_idx) DUCKDB_REQUIRES(version_lock);
};

} // namespace duckdb
