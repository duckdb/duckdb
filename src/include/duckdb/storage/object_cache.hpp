//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/object_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/enums/memory_tag.hpp"
#include "duckdb/common/lru_cache.hpp"
#include "duckdb/common/memory_context.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/buffer/buffer_pool_reservation.hpp"

namespace duckdb {
class ClientContext;
class DatabaseInstance;
class BoundObjectCache;

struct BufferPoolPayload {
	explicit BufferPoolPayload(unique_ptr<TempBufferPoolReservation> &&res) : reservation(std::move(res)) {
	}
	~BufferPoolPayload() {
		reservation->Resize(0);
	}
	idx_t GetWeight() const {
		return reservation->size;
	}
	unique_ptr<BufferPoolReservation> reservation;
};

// Forward declaration.
class BufferPool;

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

struct CleanupBufferPool {
	void operator()(unique_ptr<BufferPoolReservation> &buffer) {
		D_ASSERT(buffer);
		buffer->Resize(0);
	}
};

//! Object cache is shared among multiple database instances. Entries are scoped to their owning memory context.
class ObjectCache {
public:
	//! Default max memory 8GiB for non-evictable cache entries.
	static constexpr idx_t DEFAULT_MAX_MEMORY = 8ULL * 1024 * 1024 * 1024;

	explicit ObjectCache(BufferPool &buffer_pool_p) : ObjectCache(DEFAULT_MAX_MEMORY, buffer_pool_p) {
	}

	ObjectCache(idx_t max_memory, BufferPool &buffer_pool_p) : lru_cache(max_memory), buffer_pool(buffer_pool_p) {
	}

private:
	friend class BoundObjectCache;
	friend class DatabaseInstance;

	struct ObjectCacheKey {
		MemoryContextId context_id;
		string key;

		bool operator==(const ObjectCacheKey &other) const {
			return context_id == other.context_id && key == other.key;
		}
	};

	struct ObjectCacheKeyHash {
		size_t operator()(const ObjectCacheKey &key) const {
			return std::hash<hugeint_t> {}(key.context_id.GetUUID()) ^ std::hash<string> {}(key.key);
		}
	};

	shared_ptr<ObjectCacheEntry> GetObject(MemoryContextId context_id, const string &key) {
		const lock_guard<mutex> lock(lock_mutex);
		auto cache_key = MakeCacheKey(context_id, key);
		auto non_evictable_it = non_evictable_entries.find(cache_key);
		if (non_evictable_it != non_evictable_entries.end()) {
			return non_evictable_it->second;
		}
		return lru_cache.Get(cache_key);
	}

	template <class T>
	shared_ptr<T> Get(MemoryContextId context_id, const string &key) {
		shared_ptr<ObjectCacheEntry> object = GetObject(context_id, key);
		if (!object || object->GetObjectType() != T::ObjectType()) {
			return nullptr;
		}
		return shared_ptr_cast<ObjectCacheEntry, T>(object);
	}

	template <class T, class... ARGS>
	shared_ptr<T> GetOrCreate(MemoryContextId context_id, const string &key, ARGS &&...args) {
		const lock_guard<mutex> lock(lock_mutex);
		auto cache_key = MakeCacheKey(context_id, key);

		// Check non-evictable entries first
		auto non_evictable_it = non_evictable_entries.find(cache_key);
		if (non_evictable_it != non_evictable_entries.end()) {
			auto &existing = non_evictable_it->second;
			if (existing->GetObjectType() != T::ObjectType()) {
				return nullptr;
			}
			return shared_ptr_cast<ObjectCacheEntry, T>(existing);
		}

		// Check evictable cache
		auto existing = lru_cache.Get(cache_key);
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
			non_evictable_entries[cache_key] = value;
			return value;
		}

		auto reservation =
		    make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, estimated_memory.GetIndex());
		lru_cache.Put(std::move(cache_key), value, std::move(reservation));
		return value;
	}

	void Put(MemoryContextId context_id, const string &key, shared_ptr<ObjectCacheEntry> value) {
		if (!value) {
			return;
		}

		const lock_guard<mutex> lock(lock_mutex);
		auto cache_key = MakeCacheKey(context_id, key);
		const auto estimated_memory = value->GetEstimatedCacheMemory();
		const bool is_evictable = estimated_memory.IsValid();
		if (!is_evictable) {
			non_evictable_entries[std::move(cache_key)] = std::move(value);
			return;
		}

		auto reservation =
		    make_uniq<TempBufferPoolReservation>(MemoryTag::OBJECT_CACHE, buffer_pool, estimated_memory.GetIndex());
		lru_cache.Put(std::move(cache_key), std::move(value), std::move(reservation));
	}

	void Delete(MemoryContextId context_id, const string &key) {
		const lock_guard<mutex> lock(lock_mutex);
		auto cache_key = MakeCacheKey(context_id, key);
		auto iter = non_evictable_entries.find(cache_key);
		if (iter != non_evictable_entries.end()) {
			non_evictable_entries.erase(iter);
			return;
		}
		lru_cache.Delete(cache_key);
	}

	void DropNonEvictableEntries(MemoryContextId context_id) {
		const lock_guard<mutex> lock(lock_mutex);
		for (auto entry = non_evictable_entries.begin(); entry != non_evictable_entries.end();) {
			if (entry->first.context_id == context_id) {
				entry = non_evictable_entries.erase(entry);
			} else {
				++entry;
			}
		}
	}

	//! Type-prefixed variants of the methods above. These namespace the caller-provided key with the entry's
	//! ObjectType so that callers can pass a natural key (e.g. a file path) without having to build a unique
	//! cache key themselves.
	template <class T>
	shared_ptr<T> GetWithTypePrefix(MemoryContextId context_id, const string &key) {
		return Get<T>(context_id, MakeTypedCacheKey<T>(key));
	}

	template <class T, class... ARGS>
	shared_ptr<T> GetOrCreateWithTypePrefix(MemoryContextId context_id, const string &key, ARGS &&...args) {
		return GetOrCreate<T>(context_id, MakeTypedCacheKey<T>(key), std::forward<ARGS>(args)...);
	}

	template <class T>
	void PutWithTypePrefix(MemoryContextId context_id, const string &key,
	                       shared_ptr<ObjectCacheEntry> value) { // NOLINT(performance-unnecessary-value-param)
		Put(context_id, MakeTypedCacheKey<T>(key), std::move(value));
	}

	template <class T>
	void DeleteWithTypePrefix(MemoryContextId context_id, const string &key) {
		Delete(context_id, MakeTypedCacheKey<T>(key));
	}

public:
	DUCKDB_API static ObjectCache &GetObjectCache(ClientContext &context);
	DUCKDB_API static BoundObjectCache Get(ClientContext &context);
	DUCKDB_API static BoundObjectCache Get(DatabaseInstance &db);

	idx_t GetMaxMemory() const {
		const lock_guard<mutex> lock(lock_mutex);
		return lru_cache.Capacity();
	}
	idx_t GetCurrentMemory() const {
		const lock_guard<mutex> lock(lock_mutex);
		return lru_cache.CurrentTotalWeight();
	}
	size_t GetEntryCount() const {
		const lock_guard<mutex> lock(lock_mutex);
		return lru_cache.Size() + non_evictable_entries.size();
	}
	bool IsEmpty() const {
		const lock_guard<mutex> lock(lock_mutex);
		return lru_cache.IsEmpty() && non_evictable_entries.empty();
	}

	idx_t EvictToReduceMemory(idx_t target_bytes) {
		const lock_guard<mutex> lock(lock_mutex);
		return lru_cache.EvictToReduceAtLeast(target_bytes);
	}

private:
	//! Build the internal cache key for a typed entry by namespacing the caller-provided key with the entry's
	//! ObjectType.
	template <class T>
	static string MakeTypedCacheKey(const string &key) {
		return StringUtil::Format("%s-%s", T::ObjectType(), key);
	}
	static ObjectCacheKey MakeCacheKey(MemoryContextId context_id, const string &key) {
		return ObjectCacheKey {context_id, key};
	}

private:
	mutable mutex lock_mutex;
	//! LRU cache for evictable entries
	SharedLruCache<ObjectCacheKey, ObjectCacheEntry, duckdb::BufferPoolPayload, ObjectCacheKeyHash> lru_cache;
	//! Separate storage for non-evictable entries (i.e., encryption keys)
	unordered_map<ObjectCacheKey, shared_ptr<ObjectCacheEntry>, ObjectCacheKeyHash> non_evictable_entries;
	//! Used to create buffer pool reservation on entries creation.
	BufferPool &buffer_pool;
};

class BoundObjectCache {
public:
	shared_ptr<ObjectCacheEntry> GetObject(const string &key) {
		return cache.GetObject(context_id, key);
	}

	template <class T>
	shared_ptr<T> Get(const string &key) {
		return cache.Get<T>(context_id, key);
	}

	template <class T, class... ARGS>
	shared_ptr<T> GetOrCreate(const string &key, ARGS &&...args) {
		return cache.GetOrCreate<T>(context_id, key, std::forward<ARGS>(args)...);
	}

	void Put(const string &key, shared_ptr<ObjectCacheEntry> value) {
		cache.Put(context_id, key, std::move(value));
	}

	void Delete(const string &key) {
		cache.Delete(context_id, key);
	}

	template <class T>
	shared_ptr<T> GetWithTypePrefix(const string &key) {
		return cache.GetWithTypePrefix<T>(context_id, key);
	}

	template <class T, class... ARGS>
	shared_ptr<T> GetOrCreateWithTypePrefix(const string &key, ARGS &&...args) {
		return cache.GetOrCreateWithTypePrefix<T>(context_id, key, std::forward<ARGS>(args)...);
	}

	template <class T>
	void PutWithTypePrefix(const string &key,
	                       shared_ptr<ObjectCacheEntry> value) { // NOLINT(performance-unnecessary-value-param)
		cache.PutWithTypePrefix<T>(context_id, key, std::move(value));
	}

	template <class T>
	void DeleteWithTypePrefix(const string &key) {
		cache.DeleteWithTypePrefix<T>(context_id, key);
	}

private:
	friend class ObjectCache;

	BoundObjectCache(ObjectCache &cache_p, MemoryContextId context_id_p) : cache(cache_p), context_id(context_id_p) {
	}

	ObjectCache &cache;
	MemoryContextId context_id;
};

} // namespace duckdb
