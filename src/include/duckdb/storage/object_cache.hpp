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
#include "duckdb/common/string.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {

//! ObjectCache is the base class for objects caches in DuckDB
class ObjectCacheEntry {
public:
	//! Constant used to indicate the entry should never be evicted
	static constexpr idx_t NON_EVICTABLE = NumericLimits<idx_t>::Maximum();

	virtual ~ObjectCacheEntry() {
	}

	virtual string GetObjectType() = 0;

	//! Get the rough cache memory usage in bytes for this entry.
	//! Used for eviction decisions. Return ObjectCacheEntry::NON_EVICTABLE to prevent eviction.
	virtual idx_t GetRoughCacheMemory() const = 0;
};

class ObjectCache {
public:
	//! Default max memory 2GiB for non-evictable cache entries.
	//
	// TODO(hjiang): Hard-code a large enough memory consumption upper bound, which is likely a non-regression change.
	// I will followup with another PR before v1.5.0 release to provide a user option to tune.
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
		auto existing = GetObject(key);
		if (existing && existing->GetObjectType() == T::ObjectType()) {
			return shared_ptr_cast<ObjectCacheEntry, T>(existing);
		}

		auto value = make_shared_ptr<T>(args...);
		Put(key, value);
		return value;
	}

	void Put(string key, shared_ptr<ObjectCacheEntry> value) {
		if (!value) {
			return;
		}
		const lock_guard<mutex> lock(lock_mutex);
		bool is_non_evictable = value->GetRoughCacheMemory() == ObjectCacheEntry::NON_EVICTABLE;
		if (is_non_evictable) {
			non_evictable_entries[std::move(key)] = std::move(value);
			return;
		}
		lru_cache.Put(key, std::move(value));
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
		return lru_cache.MaxMemory();
	}
	idx_t GetCurrentMemory() const {
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
