#include "duckdb/storage/external_file_cache/external_file_cache.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"

namespace duckdb {

idx_t ExternalFileCache::GetCacheBlockSize(const string &path) {
	return FileSystem::IsRemoteFile(path) ? REMOTE_FILE_CACHE_BLOCK_SIZE : LOCAL_FILE_CACHE_BLOCK_SIZE;
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
	if (!Timestamp::IsFinite(current_last_modified) || !Timestamp::IsFinite(cached_last_modified)) {
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
	if (!access_time.TrySubtract(current_last_modified, last_modified_time)) {
		// ouf ot range
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
