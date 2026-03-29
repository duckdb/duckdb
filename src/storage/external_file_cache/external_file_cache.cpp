#include "duckdb/storage/external_file_cache/external_file_cache.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/file_system.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/settings.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

idx_t ExternalFileCache::GetCacheBlockSize(const string &path) const {
	auto &db = buffer_manager.GetDatabase();
	if (FileSystem::IsRemoteFile(path)) {
		return Settings::Get<ExternalFileCacheRemoteBlockSizeSetting>(db);
	}
	return Settings::Get<ExternalFileCacheLocalBlockSizeSetting>(db);
}

void ExternalFileCache::ReindexCachedFile(CachedFile &cached_file, idx_t old_block_size, idx_t new_block_size) {
	idx_t file_size = 0;
	{
		annotated_lock_guard<annotated_mutex> meta_guard(cached_file.meta_lock);
		file_size = cached_file.file_size;
	}
	if (file_size == 0) {
		return;
	}

	const annotated_lock_guard<annotated_mutex> map_guard(cached_file.map_lock);

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
		// Find a contiguous run of old blocks starting at 'it'.
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
			if (new_start >= new_end || new_start < run_byte_start || new_end > run_byte_end) {
				continue;
			}
			const idx_t new_size = new_end - new_start;

			auto buf = buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, new_size);

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
				memcpy(buf.Ptr() + (copy_start - new_start), old_entry.first.Ptr() + (copy_start - oi_file_start),
				       copy_end - copy_start);
			}

			auto new_block = make_shared_ptr<CacheBlock>();
			{
				const annotated_lock_guard<annotated_mutex> block_guard(new_block->mtx);
				new_block->block_handle = buf.GetBlockHandle();
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

void ExternalFileCache::ReindexCachedFiles(bool is_remote, idx_t old_block_size, idx_t new_block_size) {
	D_ASSERT(old_block_size > 0);
	D_ASSERT(new_block_size > 0);
	if (old_block_size == new_block_size) {
		return;
	}

	lock_guard<mutex> guard(lock);
	for (auto &entry : cached_files) {
		if (FileSystem::IsRemoteFile(entry.first) != is_remote) {
			continue;
		}
		ReindexCachedFile(*entry.second, old_block_size, new_block_size);
	}
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
		return cached_version_tag == current_version_tag; // Validity checked by version tag (httpfs)
	}
	if (cached_last_modified != current_last_modified) {
		return false; // The file has certainly been modified
	}
	// The last modified time matches. However, we cannot blindly trust this,
	// because some file systems use a low resolution clock to set the last modified time.
	// So, we will require that the last modified time is more than 10 seconds ago.
	static constexpr int64_t LAST_MODIFIED_THRESHOLD = 10LL * 1000LL * 1000LL;
	const auto access_time = Timestamp::GetCurrentTimestamp();
	if (access_time < current_last_modified) {
		return false; // Last modified in the future?
	}
	return access_time - current_last_modified > LAST_MODIFIED_THRESHOLD;
}

ExternalFileCache::ExternalFileCache(DatabaseInstance &db, bool enable_p)
    : buffer_manager(BufferManager::GetBufferManager(db)), enable(enable_p) {
}

bool ExternalFileCache::IsEnabled() const {
	return enable;
}

void ExternalFileCache::SetEnabled(bool enable_p) {
	lock_guard<mutex> guard(lock);
	enable = enable_p;
	if (!enable) {
		cached_files.clear();
	}
}

vector<CachedFileInformation> ExternalFileCache::GetCachedFileInformation() const {
	unique_lock<mutex> files_guard(lock);
	vector<CachedFileInformation> result;
	for (const auto &file : cached_files) {
		const idx_t block_size = GetCacheBlockSize(file.first);
		annotated_lock_guard<annotated_mutex> map_guard(file.second->map_lock);
		for (const auto &block_entry : file.second->blocks) {
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

ExternalFileCache::CachedFile &ExternalFileCache::GetOrCreateCachedFile(const string &path) {
	lock_guard<mutex> guard(lock);
	auto &entry = cached_files[path];
	if (!entry) {
		entry = make_uniq<CachedFile>(path);
	}
	return *entry;
}

} // namespace duckdb
