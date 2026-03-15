//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/external_file_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr_ipp.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/thread_annotation.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/winapi.hpp"
#include "duckdb/storage/buffer/temporary_file_information.hpp"

#include <condition_variable>

namespace duckdb {

// Forward declaration.
class ClientContext;
class DatabaseInstance;
class BlockHandle;
class BufferManager;

enum class CacheBlockState : uint8_t {
	EMPTY,   // no data, no one fetching
	LOADING, // a thread is actively performing I/O
	LOADED,  // data available in block_handle (may be evicted by buffer manager)
	ERROR    // I/O failed, error_message contains the reason
};

struct CacheBlock {
	mutable annotated_mutex mtx;
	mutable std::condition_variable cv;
	CacheBlockState state DUCKDB_GUARDED_BY(mtx) = CacheBlockState::EMPTY;
	shared_ptr<BlockHandle> block_handle DUCKDB_GUARDED_BY(mtx);
	string error_message DUCKDB_GUARDED_BY(mtx);
};

class ExternalFileCache {
public:
	static constexpr idx_t CACHE_BLOCK_SIZE = 2ULL * 1024 * 1024; // 2 MiB

	//! Cached files
	struct CachedFile {
	public:
		explicit CachedFile(string path_p);

	public:
		//! Whether the CachedFile is still valid given the current modified/version tag
		bool IsValid(bool validate, const string &current_version_tag, timestamp_t current_last_modified);

		const string path;

		mutable annotated_mutex map_lock;
		unordered_map<idx_t, shared_ptr<CacheBlock>> blocks DUCKDB_GUARDED_BY(map_lock);

		mutable annotated_mutex meta_lock;
		idx_t file_size DUCKDB_GUARDED_BY(meta_lock) = 0;
		timestamp_t last_modified DUCKDB_GUARDED_BY(meta_lock) = timestamp_t(0);
		string version_tag DUCKDB_GUARDED_BY(meta_lock);
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
	vector<CachedFileInformation> GetCachedFileInformation() const;

	BufferManager &GetBufferManager() const;
	//! Gets the cached file, or creates it if is not yet present
	CachedFile &GetOrCreateCachedFile(const string &path);

	DUCKDB_API static bool IsValid(bool validate, const string &cached_version_tag, timestamp_t cached_last_modified,
	                               const string &current_version_tag, timestamp_t current_last_modified);

private:
	//! The BufferManager used to cache files
	BufferManager &buffer_manager;
	//! Whether or not file caching is enabled
	atomic<bool> enable;
	//! Mapping from file path to cached file with cached blocks
	unordered_map<string, unique_ptr<CachedFile>> cached_files;
	//! Lock for accessing the cached files
	mutable mutex lock;
};

} // namespace duckdb
