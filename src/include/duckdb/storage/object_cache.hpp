//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/storage/object_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"

namespace duckdb {
class ClientContext;

//! ObjectCache is the base class for objects caches in DuckDB
class ObjectCacheEntry {
public:
	virtual ~ObjectCacheEntry() {
	}

	virtual string GetObjectType() = 0;
};

class ObjectCache {
public:
	shared_ptr<ObjectCacheEntry> GetObject(const string &key) {
		lock_guard<mutex> glock(lock);
		auto entry = cache.find(key);
		if (entry == cache.end()) {
			return nullptr;
		}
		return entry->second;
	}

	template <class T>
	shared_ptr<T> Get(const string &key) {
		shared_ptr<ObjectCacheEntry> object = GetObject(key);
		if (!object || object->GetObjectType() != T::ObjectType()) {
			return nullptr;
		}
		return std::static_pointer_cast<T, ObjectCacheEntry>(object);
	}

	template <class T, class... Args>
	shared_ptr<T> GetOrCreate(const string &key, Args &&...args) {
		lock_guard<mutex> glock(lock);

		auto entry = cache.find(key);
		if (entry == cache.end()) {
			auto value = make_shared<T>(args...);
			cache[key] = value;
			return value;
		}
		auto object = entry->second;
		if (!object || object->GetObjectType() != T::ObjectType()) {
			return nullptr;
		}
		return std::static_pointer_cast<T, ObjectCacheEntry>(object);
	}

	void Put(string key, shared_ptr<ObjectCacheEntry> value) {
		lock_guard<mutex> glock(lock);
		cache[key] = std::move(value);
	}

	void Delete(const string &key) {
		lock_guard<mutex> glock(lock);
		cache.erase(key);
	}

	DUCKDB_API static ObjectCache &GetObjectCache(ClientContext &context);
	DUCKDB_API static bool ObjectCacheEnabled(ClientContext &context);

private:
	//! Object Cache
	unordered_map<string, shared_ptr<ObjectCacheEntry>> cache;
	mutex lock;
};

} // namespace duckdb
