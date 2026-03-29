#include "duckdb/storage/external_file_cache/external_file_cache.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/file_system.hpp"
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

void ExternalFileCache::ClearCachedFiles() {
	lock_guard<mutex> guard(lock);
	cached_files.clear();
}

void ExternalFileCache::ReindexCachedFiles(bool is_remote, idx_t old_block_size, idx_t new_block_size) {
	D_ASSERT(old_block_size > 0 && new_block_size > 0);
	if (old_block_size == new_block_size) {
		return;
	}

	lock_guard<mutex> guard(lock);
	for (auto &entry : cached_files) {
		const auto &path = entry.first;
		if (FileSystem::IsRemoteFile(path) != is_remote) {
			continue;
		}
		auto &cached_file = *entry.second;

		idx_t file_size;
		{
			annotated_lock_guard<annotated_mutex> meta_guard(cached_file.meta_lock);
			file_size = cached_file.file_size;
		}
		if (file_size == 0) {
			continue;
		}

		annotated_lock_guard<annotated_mutex> map_guard(cached_file.map_lock);

		// Phase 1: Pin all LOADED old blocks.
		unordered_map<idx_t, pair<BufferHandle, idx_t>> pinned;
		for (auto &block_entry : cached_file.blocks) {
			const idx_t old_idx = block_entry.first;
			auto &block = *block_entry.second;
			annotated_lock_guard<annotated_mutex> block_guard(block.mtx);
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
			continue;
		}

		// Phase 2: Build new blocks by copying data from pinned old blocks.
		unordered_map<idx_t, shared_ptr<CacheBlock>> new_blocks;

		for (auto &pinned_entry : pinned) {
			const idx_t old_idx = pinned_entry.first;
			const idx_t old_nr_bytes = pinned_entry.second.second;
			const idx_t old_start = old_idx * old_block_size;

			const idx_t new_first = old_start / new_block_size;
			const idx_t new_last = (old_start + old_nr_bytes - 1) / new_block_size;

			for (idx_t new_idx = new_first; new_idx <= new_last; new_idx++) {
				if (new_blocks.count(new_idx)) {
					continue;
				}

				const idx_t new_start = new_idx * new_block_size;
				const idx_t new_end = MinValue(new_start + new_block_size, file_size);
				if (new_start >= new_end) {
					continue;
				}
				const idx_t new_size = new_end - new_start;

				// Check that ALL contributing old blocks are pinned and have enough bytes.
				const idx_t contrib_first = new_start / old_block_size;
				const idx_t contrib_last = (new_end - 1) / old_block_size;
				bool all_available = true;
				for (idx_t oi = contrib_first; oi <= contrib_last; oi++) {
					if (!pinned.count(oi)) {
						all_available = false;
						break;
					}
					const idx_t oi_file_start = oi * old_block_size;
					const idx_t need_up_to = MinValue(new_end, oi_file_start + old_block_size);
					if (oi_file_start + pinned.at(oi).second < need_up_to) {
						all_available = false;
						break;
					}
				}
				if (!all_available) {
					continue;
				}

				// Allocate new buffer and copy from contributing old block(s).
				auto buf = buffer_manager.Allocate(MemoryTag::EXTERNAL_FILE_CACHE, new_size);

				for (idx_t oi = contrib_first; oi <= contrib_last; oi++) {
					auto &old_pin = pinned.at(oi).first;
					const idx_t old_pin_bytes = pinned.at(oi).second;
					const idx_t oi_file_start = oi * old_block_size;

					const idx_t copy_start = MaxValue(new_start, oi_file_start);
					const idx_t copy_end = MinValue(new_end, oi_file_start + old_pin_bytes);
					if (copy_start >= copy_end) {
						continue;
					}
					const idx_t src_offset = copy_start - oi_file_start;
					const idx_t dst_offset = copy_start - new_start;
					const idx_t copy_len = copy_end - copy_start;
					memcpy(buf.Ptr() + dst_offset, old_pin.Ptr() + src_offset, copy_len);
				}

				auto new_block = make_shared_ptr<CacheBlock>();
				{
					annotated_lock_guard<annotated_mutex> block_guard(new_block->mtx);
					new_block->block_handle = buf.GetBlockHandle();
					new_block->nr_bytes = new_size;
					new_block->state = CacheBlockState::LOADED;
#ifdef DEBUG
					new_block->checksum = Checksum(buf.Ptr(), new_size);
#endif
				}
				new_blocks[new_idx] = std::move(new_block);
			}
		}

		// Phase 3: Replace old blocks with new blocks.
		cached_file.blocks = std::move(new_blocks);
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
		const idx_t block_size = this->GetCacheBlockSize(file.first);
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
