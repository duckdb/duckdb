//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache/external_file_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/buffer/buffer_pool_reservation.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"
#include "duckdb/storage/external_file_cache/external_file_cache_block.hpp"

namespace duckdb {

// Forward declaration.
class ClientContext;
class DatabaseInstance;
class BufferManager;
class BufferPool;
struct NetworkThroughputEstimate;

//! File metadata used to determine whether cached file data is still valid.
struct CacheValidationInfo {
	//! Whether the backend explicitly prohibits reuse. Validators cannot make such an entry reusable.
	bool IsCacheReuseProhibited() const;
	//! Whether the freshness deadline has passed, including explicitly stale entries.
	//! An ordinarily expired entry can still be revalidated using validators.
	bool IsExpired() const;

	//! Version tag (e.g., HTTP ETag). Empty if the storage backend does not provide one.
	string version_tag;
	//! Last modified time. Zero/non-finite if the storage backend does not provide one.
	timestamp_t last_modified = timestamp_t(0);
	//! Freshness deadline for files without validators (e.g., HTTP Cache-Control).
	//! The deadline is exclusive: cached data may be served only while the current time is before it.
	//! Unset means the storage backend does not provide expiry information.
	//! Positive/negative infinity mean always valid/invalid.
	optional<timestamp_t> cache_valid_until;
	idx_t file_size = 0;
};

class ExternalFileCache {
public:
	//! Blocks sized from the network are at most this large
	static constexpr idx_t ADAPTIVE_MAX_BLOCK_SIZE = 32ULL << 20;
	//! Blocks sized from the network take at least this many times the request latency to transfer
	static constexpr idx_t ADAPTIVE_LATENCY_FACTOR = 4;
	//! Reads of readers that rely on the split for parallelism keep at least this many requests per read
	static constexpr idx_t ADAPTIVE_MIN_BLOCKS_PER_READ = 8;

public:
	//! Get the maximum cache block size for a given file path.
	DUCKDB_API idx_t GetCacheMaxBlockSize(const string &path) const;
	//! Get the block size that reads of a given file path are rounded out to.
	DUCKDB_API idx_t GetCacheMinBlockSize(const string &path) const;
	//! Whether reads of the given file should go through the cache (remote files only, unless forced).
	DUCKDB_API bool ShouldCacheFile(const string &path) const;

	//! The size of the blocks a read of [nr_bytes] of a remote file is split into, given the measured latency and
	//! bandwidth of its requests. The smallest multiple of [block_size] that keeps the request latency a small part
	//! of each request, but no larger than leaves [min_blocks_per_read] requests for the read. [block_size] without
	//! a usable estimate.
	DUCKDB_API static idx_t GetAdaptiveBlockSize(idx_t nr_bytes, idx_t block_size,
	                                             const NetworkThroughputEstimate &estimate,
	                                             idx_t min_blocks_per_read = 1);
	//! Remember the latest throughput estimate of the host of a remote file.
	void SetHostNetworkThroughput(const string &path, const NetworkThroughputEstimate &estimate);
	//! The latest throughput estimate of the host of a remote file, for handles that have not measured their own yet.
	bool TryGetHostNetworkThroughput(const string &path, NetworkThroughputEstimate &result) const;

	//! Cached files
	struct CachedFile {
	public:
		CachedFile(string path_p, idx_t generation_p, BufferPool &buffer_pool);

	public:
		//! Resize the block metadata reservation to the current number of blocks.
		void UpdateBlockReservation() DUCKDB_REQUIRES(map_lock);

	public:
		const string path;
		const idx_t generation;

		mutable annotated_mutex map_lock;
		//! Non-overlapping cached blocks, keyed by file offset.
		map<idx_t, shared_ptr<CacheBlock>> blocks DUCKDB_GUARDED_BY(map_lock);
		//! Incremented whenever the blocks of the file are dropped.
		idx_t content_generation DUCKDB_GUARDED_BY(map_lock) = 0;
		//! Memory reserved for the metadata of the blocks.
		TempBufferPoolReservation block_reservation DUCKDB_GUARDED_BY(map_lock);

		mutable annotated_mutex meta_lock;
		//! Metadata for validating the cached blocks against the current file.
		CacheValidationInfo validation_info DUCKDB_GUARDED_BY(meta_lock);
		bool can_seek DUCKDB_GUARDED_BY(meta_lock) = false;
		bool on_disk_file DUCKDB_GUARDED_BY(meta_lock) = false;
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

	//! Get the blocks covering [location, location + nr_bytes), creating empty blocks for the missing bytes.
	vector<shared_ptr<CacheBlock>> AcquireBlocks(CachedFile &cached_file, idx_t location, idx_t nr_bytes,
	                                             idx_t max_block_size);
	//! Drop all blocks of a file, e.g. because the file changed. Readers keep the blocks they hold.
	void DropBlocks(CachedFile &cached_file);
	idx_t GetContentGeneration(CachedFile &cached_file);

	BufferManager &GetBufferManager() const;
	//! Gets the shared cached file for the given path, creating it if not yet present.
	//! When caching is disabled, returns a transient CachedFile that is not tracked in the cached file map.
	shared_ptr<CachedFile> GetOrCreateCachedFile(const string &path);

	//! Allocate a buffer holding a cache block of the given file. Blocks of remote files spill to the
	//! temporary directory when they are evicted, instead of being dropped and re-fetched from the
	//! source. Local files can be re-read at the same cost as a spilled block, so they are always
	//! dropped, as are blocks below the block allocation size, which would each need their own file.
	static BufferHandle AllocateCacheBuffer(BufferManager &buffer_manager, const string &path, idx_t nr_bytes);

	DUCKDB_API static bool IsValid(bool validate, const string &cached_version_tag, timestamp_t cached_last_modified,
	                               const string &current_version_tag, timestamp_t current_last_modified);
	//! Variant that can also validate files without validators using the freshness deadline and file size
	DUCKDB_API static bool IsValid(bool validate, const CacheValidationInfo &cached,
	                               const CacheValidationInfo &current);
	//! Whether the version tag/last modified time provide any cache validation metadata
	DUCKDB_API static bool HasValidationMetadata(const CacheValidationInfo &info);

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
	unordered_map<string, idx_t> cached_file_keys DUCKDB_GUARDED_BY(lock);
	//! Lock for accessing cached_file_keys.
	mutable annotated_mutex lock;

	//! A measured request latency and bandwidth
	struct HostThroughput {
		double latency_seconds;
		double bandwidth_bytes_per_s;
	};
	//! Latest throughput estimate per remote host (scheme and authority of the path)
	unordered_map<string, HostThroughput> host_throughput DUCKDB_GUARDED_BY(throughput_lock);
	//! Lock for accessing host_throughput.
	mutable annotated_mutex throughput_lock;
};

} // namespace duckdb
