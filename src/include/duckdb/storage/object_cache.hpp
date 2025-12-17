//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/object_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/lru_cache.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

//! ObjectCache is the base class for objects caches in DuckDB
class ObjectCacheEntry {
public:
	virtual ~ObjectCacheEntry() {
	}

	virtual string GetObjectType() = 0;

	//! Get the rough cache memory usage in bytes for this entry.
	//! Used for eviction decisions. Return invalid index to prevent eviction.
	virtual optional_idx GetEstimatedCacheMemory() const = 0;
};

class ObjectCache {
public:
	//! Default max memory 8GiB for non-evictable cache entries.
	//
	// TODO(hjiang): Hard-code a large enough memory consumption upper bound, which is likely a non-regression change.
	// I will followup with another PR before v1.5.0 release to provide a user option to tune.
	//
	// A few consideration here: should we cap object cache memory consumption with duckdb max memory or separate.
	static constexpr idx_t DEFAULT_MAX_MEMORY = 8ULL * 1024 * 1024 * 1024;

	ObjectCache() : ObjectCache(DEFAULT_MAX_MEMORY) {
	}

	explicit ObjectCache(idx_t max_memory) : lru_cache(max_memory) {
	}

	shared_ptr<ObjectCacheEntry> GetObject(const string &key) {
		const lock_guard<mutex> lock(lock_mutex);
		auto non_evictable_it = non_evictable_entries.find(key);
		if (non_evictable_it != non_evictable_entries.end()) {
			return non_evictable_it->second;
		}
		return lru_cache.Get(key);
	}

	template <class T>
	shared_ptr<T> Get(const string &key) {
		shared_ptr<ObjectCacheEntry> object = GetObject(key);
		if (!object || object->GetObjectType() != T::ObjectType()) {
			return nullptr;
		}
		return shared_ptr_cast<ObjectCacheEntry, T>(object);
	}

	template <class T, class... ARGS>
	shared_ptr<T> GetOrCreate(const string &key, ARGS &&... args) {
		const lock_guard<mutex> lock(lock_mutex);

		// Check non-evictable entries first
		auto non_evictable_it = non_evictable_entries.find(key);
		if (non_evictable_it != non_evictable_entries.end()) {
			auto &existing = non_evictable_it->second;
			if (existing->GetObjectType() != T::ObjectType()) {
				return nullptr;
			}
			return shared_ptr_cast<ObjectCacheEntry, T>(existing);
		}

		// Check evictable cache
		auto existing = lru_cache.Get(key);
		if (existing) {
			if (existing->GetObjectType() != T::ObjectType()) {
				return nullptr;
			}
			return shared_ptr_cast<ObjectCacheEntry, T>(existing);
		}

		// Create new entry while holding lock
		auto value = make_shared_ptr<T>(args...);
		const auto estimated_memory = value->GetEstimatedCacheMemory();
		const bool is_evictable = estimated_memory.IsValid();
		if (!is_evictable) {
			non_evictable_entries[key] = value;
		} else {
			lru_cache.Put(key, value, estimated_memory.GetIndex());
		}

		return value;
	}

	void Put(string key, shared_ptr<ObjectCacheEntry> value) {
		if (!value) {
			return;
		}

		const lock_guard<mutex> lock(lock_mutex);
		const auto estimated_memory = value->GetEstimatedCacheMemory();
		const bool is_evictable = estimated_memory.IsValid();
		if (!is_evictable) {
			non_evictable_entries[std::move(key)] = std::move(value);
			return;
		}
		lru_cache.Put(key, std::move(value), estimated_memory.GetIndex());
	}

	void Delete(const string &key) {
		const lock_guard<mutex> lock(lock_mutex);
		auto iter = non_evictable_entries.find(key);
		if (iter != non_evictable_entries.end()) {
			non_evictable_entries.erase(iter);
			return;
		}
		lru_cache.Delete(key);
	}

	DUCKDB_API static ObjectCache &GetObjectCache(ClientContext &context);

	idx_t GetMaxMemory() const {
		const lock_guard<mutex> lock(lock_mutex);
		return lru_cache.MaxMemory();
	}
	idx_t GetCurrentMemory() const {
		const lock_guard<mutex> lock(lock_mutex);
		return lru_cache.CurrentMemory();
	}
	size_t GetEntryCount() const {
		const lock_guard<mutex> lock(lock_mutex);
		return lru_cache.Size() + non_evictable_entries.size();
	}

private:
	mutable mutex lock_mutex;
	//! LRU cache for evictable entries
	SharedLruCache<string, ObjectCacheEntry> lru_cache;
	//! Separate storage for non-evictable entries (i.e., encryption keys)
	unordered_map<string, shared_ptr<ObjectCacheEntry>> non_evictable_entries;
};

} // namespace duckdb
