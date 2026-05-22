#include "duckdb/storage/external_file_cache/external_file_cache.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/external_file_cache/external_file_cache_block_memory.hpp"

namespace duckdb {

idx_t ExternalFileCache::GetCacheBlockSize(const string &path) const {
	auto &db = buffer_manager.GetDatabase();
	if (FileSystem::IsRemoteFile(path)) {
		return Settings::Get<ExternalFileCacheRemoteBlockSizeSetting>(db);
	}
	return Settings::Get<ExternalFileCacheLocalBlockSizeSetting>(db);
}

void ExternalFileCache::ReindexCachedFileCore(CachedFile &cached_file, shared_ptr<CachedFile> cached_file_ref,
                                              idx_t file_size, idx_t old_block_size, idx_t new_block_size) {
	D_ASSERT(old_block_size > 0);
	D_ASSERT(new_block_size > 0);

	// Phase 1: Pin all LOADED old blocks, sorted by block index.
	map<idx_t, pair<BufferHandle, idx_t>> pinned;
	for (auto &block_entry : cached_file.blocks) {
		const idx_t old_idx = block_entry.first;
		auto &block = *block_entry.second;
		const annotated_lock_guard<annotated_mutex> block_guard(block.mtx);
		if (block.state != CacheBlockState::LOADED || !block.block_handle) {
			continue;
		}
		auto pin = buffer_manager.Pin(block.block_handle);
		if (pin.IsValid()) {
			pinned.emplace(old_idx, make_pair(std::move(pin), block.nr_bytes));
		}
	}

	if (pinned.empty()) {
		cached_file.blocks.clear();
		return;
	}

	// Phase 2: Find contiguous runs of old blocks and create new blocks from each run.
	// A new block is only created if its entire byte range is covered by the run.
	unordered_map<idx_t, shared_ptr<CacheBlock>> new_blocks;

	auto it = pinned.begin();
	while (it != pinned.end()) {
		// Find a contiguous run of old blocks starting at current block.
		const idx_t run_byte_start = it->first * old_block_size;
		idx_t run_byte_end = run_byte_start;
		idx_t expected_idx = it->first;
		auto run_end = it;
		while (run_end != pinned.end() && run_end->first == expected_idx) {
			run_byte_end = run_end->first * old_block_size + run_end->second.second;
			expected_idx++;
			++run_end;
		}

		// This contiguous run covers file bytes [run_byte_start, run_byte_end).
		// Create all new blocks whose byte range fits entirely within this run.
		const idx_t first_new = run_byte_start / new_block_size;
		const idx_t last_new = (run_byte_end - 1) / new_block_size;

		for (idx_t new_idx = first_new; new_idx <= last_new; new_idx++) {
			const idx_t new_start = new_idx * new_block_size;
			const idx_t new_end = MinValue(new_start + new_block_size, file_size);
			if (!(new_start < new_end && new_start >= run_byte_start && new_end <= run_byte_end)) {
				continue;
			}
			const idx_t new_size = new_end - new_start;

			auto buf = AllocateCacheBlock(cached_file_ref, new_size);

			// Copy from each contributing old block in the run.
			const idx_t contrib_first = new_start / old_block_size;
			const idx_t contrib_last = (new_end - 1) / old_block_size;
			for (idx_t oi = contrib_first; oi <= contrib_last; oi++) {
				auto &old_entry = pinned.at(oi);
				const idx_t oi_file_start = oi * old_block_size;
				const idx_t copy_start = MaxValue(new_start, oi_file_start);
				const idx_t copy_end = MinValue(new_end, oi_file_start + old_entry.second);
				if (copy_start >= copy_end) {
					continue;
				}
				memcpy(buf.GetDataMutable() + (copy_start - new_start),
				       old_entry.first.Ptr() + (copy_start - oi_file_start), copy_end - copy_start);
			}

			auto new_block = make_shared_ptr<CacheBlock>();
			auto block_handle = buf.GetBlockHandle();
			{
				const annotated_lock_guard<annotated_mutex> block_guard(new_block->mtx);
				new_block->block_handle = std::move(block_handle);
				new_block->nr_bytes = new_size;
				new_block->state = CacheBlockState::LOADED;
#ifdef DEBUG
				new_block->checksum = Checksum(buf.Ptr(), new_size);
#endif
			}
			new_blocks[new_idx] = std::move(new_block);
		}

		it = run_end;
	}

	// Phase 3: Replace old blocks with new blocks.
	cached_file.blocks = std::move(new_blocks);
}

vector<shared_ptr<CacheBlock>> ExternalFileCache::ReindexAndAcquireBlocks(shared_ptr<CachedFile> cached_file_p,
                                                                          idx_t current_block_size, idx_t first_block,
                                                                          idx_t num_blocks) {
	D_ASSERT(current_block_size > 0);
	auto &cached_file = *cached_file_p;

	idx_t file_size = 0;
	{
		const annotated_lock_guard<annotated_mutex> meta_guard(cached_file.meta_lock);
		file_size = cached_file.file_size;
	}

	const annotated_lock_guard<annotated_mutex> map_guard(cached_file.map_lock);

	if (cached_file.cached_block_size.IsValid() && cached_file.cached_block_size.GetIndex() != current_block_size) {
		const idx_t old_block_size = cached_file.cached_block_size.GetIndex();
		if (file_size > 0) {
			ReindexCachedFileCore(cached_file, cached_file_p, file_size, old_block_size, current_block_size);
		}
	}
	cached_file.cached_block_size = current_block_size;

	vector<shared_ptr<CacheBlock>> blocks(num_blocks);
	for (idx_t idx = 0; idx < num_blocks; idx++) {
		const idx_t block_idx = first_block + idx;
		auto &entry = cached_file.blocks[block_idx];
		if (!entry) {
			entry = make_shared_ptr<CacheBlock>();
		}
		blocks[idx] = entry;
	}
	return blocks;
}

ExternalFileCache::CachedFile::CachedFile(string path_p) : path(std::move(path_p)) {
}

bool ExternalFileCache::CachedFile::IsValid(bool validate, const string &current_version_tag,
                                            timestamp_t current_last_modified) {
	if (!validate) {
		return true; // Assume valid
	}
	annotated_lock_guard<annotated_mutex> guard(meta_lock);
	return ExternalFileCache::IsValid(validate, version_tag, last_modified, current_version_tag, current_last_modified);
}

bool ExternalFileCache::IsValid(bool validate, const string &cached_version_tag, timestamp_t cached_last_modified,
                                const string &current_version_tag, timestamp_t current_last_modified) {
	if (!validate) {
		return true; // Assume valid
	}
	if (!current_version_tag.empty() || !cached_version_tag.empty()) {
		return cached_version_tag == current_version_tag; // Validity checked by version tag
	}
	if (cached_last_modified != current_last_modified) {
		return false; // The file has certainly been modified
	}

	// If the modified time is not assigned (i.e., storage backend does not provide it), we can't validate it.
	if (!current_last_modified.IsFinite() || !cached_last_modified.IsFinite()) {
		return false;
	}

	// The last modified time matches. However, we cannot blindly trust this,
	// because some file systems use a low resolution clock to set the last modified time.
	// So, we will require that the last modified time is more than 10 seconds ago.
	static constexpr int64_t LAST_MODIFIED_THRESHOLD = 10LL * 1000LL * 1000LL;
	const auto access_time = Timestamp::GetCurrentTimestamp();
	if (access_time < current_last_modified) {
		return false; // Last modified in the future?
	}
	int64_t last_modified_time;
	if (!TrySubtractOperator::Operation(access_time, current_last_modified, last_modified_time)) {
		// out of range
		return false;
	}
	return last_modified_time > LAST_MODIFIED_THRESHOLD;
}

ExternalFileCache::ExternalFileCache(DatabaseInstance &db, bool enable_p)
    : buffer_manager(BufferManager::GetBufferManager(db)), enable(enable_p) {
}

bool ExternalFileCache::IsEnabled() const {
	return enable;
}

void ExternalFileCache::SetEnabled(bool enable_p) {
	unordered_map<string, CachedFileWithRefCount> cached_files_to_destroy;
	{
		lock_guard<mutex> guard(lock);
		enable = enable_p;
		if (!enable) {
			cached_files_to_destroy = std::move(cached_files);
		}
	}
}

vector<CachedFileInformation> ExternalFileCache::GetCachedFileInformation() const {
	vector<pair<string, shared_ptr<CachedFile>>> cached_file_snapshot;
	{
		unique_lock<mutex> files_guard(lock);
		cached_file_snapshot.reserve(cached_files.size());
		for (const auto &file : cached_files) {
			cached_file_snapshot.emplace_back(make_pair(file.first, file.second.cached_file));
		}
	}
	vector<CachedFileInformation> result;
	for (const auto &file : cached_file_snapshot) {
		auto &cached_file = *file.second;
		const annotated_lock_guard<annotated_mutex> map_guard(cached_file.map_lock);
		const idx_t block_size = cached_file.cached_block_size.IsValid() ? cached_file.cached_block_size.GetIndex()
		                                                                 : GetCacheBlockSize(file.first);
		for (const auto &block_entry : cached_file.blocks) {
			const idx_t block_idx = block_entry.first;
			const auto &block = *block_entry.second;

			annotated_lock_guard<annotated_mutex> block_guard(block.mtx);
			if (block.state != CacheBlockState::LOADED || !block.block_handle) {
				continue;
			}
			const idx_t location = block_idx * block_size;
			const bool loaded = !block.block_handle->GetMemory().IsUnloaded();
			result.push_back({file.first, block.nr_bytes, location, loaded});
		}
	}
	return result;
}

ExternalFileCache &ExternalFileCache::Get(DatabaseInstance &db) {
	return db.GetExternalFileCache();
}

ExternalFileCache &ExternalFileCache::Get(ClientContext &context) {
	return context.db->GetExternalFileCache();
}

BufferManager &ExternalFileCache::GetBufferManager() const {
	return buffer_manager;
}

BufferHandle ExternalFileCache::AllocateCacheBlock(shared_ptr<CachedFile> cached_file, idx_t block_size) {
	auto weak_file = weak_ptr<CachedFile>(cached_file);
	auto factory = [this, weak_file](BufferManager &buffer_manager, block_id_t block_id, MemoryTag tag,
	                                 unique_ptr<FileBuffer> buffer, DestroyBufferUpon destroy_buffer_upon, idx_t size,
	                                 BufferPoolReservation &&reservation) {
		return make_shared_ptr<ExternalFileCacheBlockMemory>(
		    buffer_manager, block_id, tag, std::move(buffer), destroy_buffer_upon, size, std::move(reservation),
		    [this, weak_file]() { RegisterLoadedBlock(weak_file); },
		    [this, weak_file]() { ReleaseLoadedBlock(weak_file); });
	};
	return buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, block_size, true, std::move(factory));
}

shared_ptr<ExternalFileCache::CachedFile> ExternalFileCache::GetOrCreateCachedFile(const string &path) {
	lock_guard<mutex> guard(lock);
	auto &entry = cached_files[path];
	if (!entry.cached_file) {
		entry.cached_file = make_shared_ptr<CachedFile>(path);
	}
	entry.active_handle_count++;
	return entry.cached_file;
}

void ExternalFileCache::ReleaseCachedFileHandle(CachedFile &cached_file) {
	lock_guard<mutex> guard(lock);
	auto entry = cached_files.find(cached_file.path);
	if (entry == cached_files.end()) {
		return;
	}
	if (entry->second.cached_file.get() != &cached_file) {
		return;
	}
	D_ASSERT(entry->second.active_handle_count > 0);
	if (entry->second.active_handle_count > 0) {
		entry->second.active_handle_count--;
	}
	TryEraseFileLocked(cached_file);
}

void ExternalFileCache::RegisterLoadedBlock(const weak_ptr<CachedFile> &cached_file) {
	auto locked_file = cached_file.lock();
	if (!locked_file) {
		return;
	}
	lock_guard<mutex> guard(lock);
	auto entry = cached_files.find(locked_file->path);
	if (entry == cached_files.end()) {
		return;
	}
	if (entry->second.cached_file.get() != locked_file.get()) {
		return;
	}
	entry->second.loaded_block_count++;
}

void ExternalFileCache::ReleaseLoadedBlock(const weak_ptr<CachedFile> &cached_file) {
	auto locked_file = cached_file.lock();
	if (!locked_file) {
		return;
	}
	lock_guard<mutex> guard(lock);
	auto entry = cached_files.find(locked_file->path);
	if (entry == cached_files.end()) {
		return;
	}
	if (entry->second.cached_file.get() != locked_file.get()) {
		return;
	}
	D_ASSERT(entry->second.loaded_block_count > 0);
	if (entry->second.loaded_block_count > 0) {
		entry->second.loaded_block_count--;
	}
	TryEraseFileLocked(*locked_file);
}

void ExternalFileCache::TryEraseFile(CachedFile &cached_file) {
	lock_guard<mutex> guard(lock);
	TryEraseFileLocked(cached_file);
}

void ExternalFileCache::TryEraseFileLocked(CachedFile &cached_file) {
	auto entry = cached_files.find(cached_file.path);
	if (entry == cached_files.end()) {
		return;
	}
	// DuckDB doesn't invalidate external file cache entries on file write and deletion, which means it's possible that
	// on old block unload, cache files entries store new cache blocks. so we need to validate whether they represent
	// the same version of the file.
	if (entry->second.cached_file.get() != &cached_file) {
		return;
	}
	if (entry->second.active_handle_count != 0 || entry->second.loaded_block_count != 0) {
		return;
	}
	cached_files.erase(entry);
}

} // namespace duckdb
