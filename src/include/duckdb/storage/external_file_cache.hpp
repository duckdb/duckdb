//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"
#include "duckdb/storage/storage_lock.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

class ClientContext;
class DatabaseInstance;
class BlockHandle;
class BufferManager;

class ExternalFileCache {
public:
	enum class CachedFileRangeOverlap { NONE, PARTIAL, FULL };

	//! Cached reads (immutable)
	struct CachedFileRange {
	public:
		CachedFileRange(shared_ptr<BlockHandle> block_handle, idx_t nr_bytes, idx_t location, string version_tag);
		~CachedFileRange();

	public:
		//! Gets the overlap between this file range and another
		CachedFileRangeOverlap GetOverlap(idx_t other_nr_bytes, idx_t other_location) const;
		CachedFileRangeOverlap GetOverlap(const CachedFileRange &other) const;

		//! Computes/verifies checksum over the buffer to ensure data was not modified (used for Verification only)
		void AddCheckSum();
		void VerifyCheckSum();

	public:
		shared_ptr<BlockHandle> block_handle;
		const idx_t nr_bytes;
		const idx_t location;
		const string version_tag;
#ifdef DEBUG
		hash_t checksum = 0;
#endif
	};

	//! Cached files
	struct CachedFile {
	public:
		CachedFile(string path_p, idx_t generation_p);

	public:
		//! Verifies that none of the ranges fully overlap (must hold the lock)
		void Verify(const unique_ptr<StorageLockKey> &guard) const;
		//! Whether the CachedFile is still valid given the current modified/version tag
		bool IsValid(const unique_ptr<StorageLockKey> &guard, bool validate, const string &current_version_tag,
		             timestamp_t current_last_modified);

		//! Get reference to properties (must hold the lock)
		idx_t &FileSize(const unique_ptr<StorageLockKey> &guard);
		timestamp_t &LastModified(const unique_ptr<StorageLockKey> &guard);
		string &VersionTag(const unique_ptr<StorageLockKey> &guard);
		bool &CanSeek(const unique_ptr<StorageLockKey> &guard);
		bool &OnDiskFile(const unique_ptr<StorageLockKey> &guard);
		map<idx_t, shared_ptr<CachedFileRange>> &Ranges(const unique_ptr<StorageLockKey> &guard);

	public:
		const string path;
		const idx_t generation;
		StorageLock lock;

	private:
		map<idx_t, shared_ptr<CachedFileRange>> ranges;

		idx_t file_size;
		timestamp_t last_modified;
		string version_tag;
		bool can_seek;
		bool on_disk_file;
	};

public:
	ExternalFileCache(DatabaseInstance &db, bool enable);

public:
	static ExternalFileCache &Get(DatabaseInstance &db);
	static ExternalFileCache &Get(ClientContext &context);

	bool IsEnabled() const;
	void SetEnabled(bool enable);
	idx_t GetGeneration() const;
	vector<CachedFileInformation> GetCachedFileInformation() const;
	//! Number of files tracked in the ObjectCache, exposed for testing.
	idx_t GetCachedFileCount() const;

	BufferManager &GetBufferManager() const;
	//! Gets the shared cached file for the given path, creating it if not yet present.
	//! When caching is disabled, returns a transient CachedFile that is not tracked in the cached file map.
	shared_ptr<CachedFile> GetOrCreateCachedFile(const string &path);

	DUCKDB_API static bool IsValid(bool validate, const string &cached_version_tag, timestamp_t cached_last_modified,
	                               const string &current_version_tag, timestamp_t current_last_modified);

private:
	class ExternalFileCacheObjectCacheEntry;

	//! Registers a cached file path in the tracked set.
	void InsertCachedFileKey(const string &path);
	//! Removes a cached file path from the tracked set.
	void EraseCachedFileKey(const string &path);
	//! Delete the ObjectCache entries for the given cached file paths.
	void DeleteObjectCacheEntries(const vector<string> &paths);

	//! The BufferManager used to cache files
	BufferManager &buffer_manager;
	//! Whether or not file caching is enabled
	atomic<bool> enable;
	//! Generation counter, incremented whenever cache enablement changes.
	atomic<idx_t> generation;
	//! Maps from path to the number of live entries for that path.
	//! A path can have multiple live entries while an evicted entry is still referenced.
	unordered_map<string, idx_t> cached_file_keys;
	//! Lock for accessing cached_file_keys.
	mutable mutex lock;
};

} // namespace duckdb
