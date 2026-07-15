#include "duckdb/storage/external_file_cache.hpp"

#include "duckdb/common/checksum.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/block_handle.hpp"
#include "duckdb/storage/object_cache.hpp"

namespace duckdb {

class ExternalFileCache::ExternalFileCacheObjectCacheEntry : public ObjectCacheEntry {
public:
	ExternalFileCacheObjectCacheEntry(ExternalFileCache &cache_p, string path_p, idx_t generation_p)
	    : cache(cache_p), cached_file(make_shared_ptr<CachedFile>(std::move(path_p), generation_p)) {
		cache.InsertCachedFileKey(cached_file->path);
	}

	~ExternalFileCacheObjectCacheEntry() override {
		cache.EraseCachedFileKey(cached_file->path);
	}

	static string ObjectType() {
		return "external_file_cache";
	}

	string GetObjectType() override {
		return ObjectType();
	}

	optional_idx GetEstimatedCacheMemory() const override {
		return cached_file->path.size() * 2;
	}

	shared_ptr<CachedFile> GetCachedFile() const {
		return cached_file;
	}

private:
	ExternalFileCache &cache;
	shared_ptr<CachedFile> cached_file;
};

ExternalFileCache::CachedFileRange::CachedFileRange(shared_ptr<BlockHandle> block_handle_p, idx_t nr_bytes_p,
                                                    idx_t location_p, string version_tag_p)
    : block_handle(std::move(block_handle_p)), nr_bytes(nr_bytes_p), location(location_p),
      version_tag(std::move(version_tag_p)) {
}

ExternalFileCache::CachedFileRange::~CachedFileRange() {
	VerifyCheckSum();
}

ExternalFileCache::CachedFileRangeOverlap
ExternalFileCache::CachedFileRange::GetOverlap(const idx_t other_nr_bytes, const idx_t other_location) const {
	const auto this_end = this->location + this->nr_bytes;
	const auto other_end = other_nr_bytes + other_location;
	if (this->location <= other_location && this_end >= other_end) {
		return CachedFileRangeOverlap::FULL;
	}
	if (this->location < other_end && other_location < this_end) {
		return CachedFileRangeOverlap::PARTIAL;
	}
	return CachedFileRangeOverlap::NONE;
}

ExternalFileCache::CachedFileRangeOverlap
ExternalFileCache::CachedFileRange::GetOverlap(const CachedFileRange &other) const {
	return GetOverlap(other.nr_bytes, other.location);
}

void ExternalFileCache::CachedFileRange::AddCheckSum() {
#ifdef DEBUG
	D_ASSERT(checksum == 0);
	auto buffer_handle = block_handle->GetMemory().GetBufferManager().Pin(block_handle);
	checksum = Checksum(buffer_handle.Ptr(), nr_bytes);
#endif
}

void ExternalFileCache::CachedFileRange::VerifyCheckSum() {
#ifdef DEBUG
	if (checksum == 0) {
		return;
	}
	auto buffer_handle = block_handle->GetMemory().GetBufferManager().Pin(block_handle);
	if (!buffer_handle.IsValid()) {
		return;
	}
	D_ASSERT(checksum == Checksum(buffer_handle.Ptr(), nr_bytes));
#endif
}

ExternalFileCache::CachedFile::CachedFile(string path_p, idx_t generation_p)
    : path(std::move(path_p)), generation(generation_p), file_size(0), last_modified(0), can_seek(false),
      on_disk_file(false) {
}

void ExternalFileCache::CachedFile::Verify(const unique_ptr<StorageLockKey> &guard) const {
#ifdef DEBUG
	for (const auto &range1 : ranges) {
		for (const auto &range2 : ranges) {
			if (range1.first == range2.first) {
				continue;
			}
			D_ASSERT(range1.second->GetOverlap(*range2.second) != CachedFileRangeOverlap::FULL);
		}
	}
#endif
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

bool ExternalFileCache::CachedFile::IsValid(const unique_ptr<StorageLockKey> &guard, bool validate,
                                            const string &current_version_tag, timestamp_t current_last_modified) {
	if (!validate) {
		return true; // Assume valid
	}
	return ExternalFileCache::IsValid(validate, VersionTag(guard), LastModified(guard), current_version_tag,
	                                  current_last_modified);
}

idx_t &ExternalFileCache::CachedFile::FileSize(const unique_ptr<StorageLockKey> &guard) {
	return file_size;
}

timestamp_t &ExternalFileCache::CachedFile::LastModified(const unique_ptr<StorageLockKey> &guard) {
	return last_modified;
}

string &ExternalFileCache::CachedFile::VersionTag(const unique_ptr<StorageLockKey> &guard) {
	return version_tag;
}

bool &ExternalFileCache::CachedFile::CanSeek(const unique_ptr<StorageLockKey> &guard) {
	return can_seek;
}

bool &ExternalFileCache::CachedFile::OnDiskFile(const unique_ptr<StorageLockKey> &guard) {
	return on_disk_file;
}

map<idx_t, shared_ptr<ExternalFileCache::CachedFileRange>> &
ExternalFileCache::CachedFile::Ranges(const unique_ptr<StorageLockKey> &guard) {
	return ranges;
}

ExternalFileCache::ExternalFileCache(DatabaseInstance &db, bool enable_p)
    : buffer_manager(BufferManager::GetBufferManager(db)), enable(enable_p), generation(0) {
}

bool ExternalFileCache::IsEnabled() const {
	return enable;
}

void ExternalFileCache::SetEnabled(bool enable_p) {
	vector<string> keys_to_delete;
	{
		const lock_guard<mutex> guard(lock);
		if (enable == enable_p) {
			return;
		}
		enable = enable_p;
		generation++;
		if (!enable) {
			keys_to_delete.reserve(cached_file_keys.size());
			for (auto &key : cached_file_keys) {
				keys_to_delete.emplace_back(key.first);
			}
		}
	}
	DeleteObjectCacheEntries(keys_to_delete);
}

idx_t ExternalFileCache::GetGeneration() const {
	return generation;
}

vector<CachedFileInformation> ExternalFileCache::GetCachedFileInformation() const {
	vector<string> keys;
	{
		const lock_guard<mutex> files_guard(lock);
		keys.reserve(cached_file_keys.size());
		for (auto &key : cached_file_keys) {
			keys.emplace_back(key.first);
		}
	}

	auto &object_cache = buffer_manager.GetDatabase().GetObjectCache();
	vector<CachedFileInformation> result;
	for (const auto &key : keys) {
		auto entry = object_cache.GetWithTypePrefix<ExternalFileCacheObjectCacheEntry>(key);
		if (!entry) {
			continue;
		}
		auto file = entry->GetCachedFile();
		auto ranges_guard = file->lock.GetSharedLock();
		for (const auto &range_entry : file->Ranges(ranges_guard)) {
			const auto &range = *range_entry.second;
			result.push_back(
			    {file->path, range.nr_bytes, range.location, !range.block_handle->GetMemory().IsUnloaded()});
		}
	}
	return result;
}

idx_t ExternalFileCache::GetCachedFileCount() const {
	const lock_guard<mutex> files_guard(lock);
	return cached_file_keys.size();
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

void ExternalFileCache::DeleteObjectCacheEntries(const vector<string> &paths) {
	auto &object_cache = buffer_manager.GetDatabase().GetObjectCache();
	for (auto &path : paths) {
		object_cache.DeleteWithTypePrefix<ExternalFileCacheObjectCacheEntry>(path);
	}
}

shared_ptr<ExternalFileCache::CachedFile> ExternalFileCache::GetOrCreateCachedFile(const string &path) {
	auto &object_cache = buffer_manager.GetDatabase().GetObjectCache();
	while (true) {
		const auto current_generation = generation.load();
		if (!enable) {
			return make_shared_ptr<CachedFile>(path, current_generation);
		}

		auto entry = object_cache.GetOrCreateWithTypePrefix<ExternalFileCacheObjectCacheEntry>(path, *this, path,
		                                                                                       current_generation);
		auto cached_file = entry->GetCachedFile();

		if (!enable) {
			object_cache.DeleteWithTypePrefix<ExternalFileCacheObjectCacheEntry>(path);
			return make_shared_ptr<CachedFile>(path, current_generation);
		}
		if (cached_file->generation != current_generation) {
			object_cache.DeleteWithTypePrefix<ExternalFileCacheObjectCacheEntry>(path);
			continue;
		}
		return cached_file;
	}
}

void ExternalFileCache::InsertCachedFileKey(const string &path) {
	const lock_guard<mutex> guard(lock);
	cached_file_keys[path]++;
}

void ExternalFileCache::EraseCachedFileKey(const string &path) {
	const lock_guard<mutex> guard(lock);
	auto entry = cached_file_keys.find(path);
	ALWAYS_ASSERT(entry != cached_file_keys.end());
	D_ASSERT(entry->second > 0);
	if (--entry->second == 0) {
		cached_file_keys.erase(entry);
	}
}

} // namespace duckdb
