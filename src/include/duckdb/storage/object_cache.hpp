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
};

class ObjectCache {
public:
	shared_ptr<ObjectCacheEntry> Get(std::string key) {
		lock_guard<mutex> glock(lock);
		auto entry = cache.find(key);
		if (entry == cache.end()) {
			return nullptr;
		}
		return entry->second;
	}

	void Put(std::string key, shared_ptr<ObjectCacheEntry> value) {
		lock_guard<mutex> glock(lock);
		cache[key] = move(value);
	}

	static ObjectCache &GetObjectCache(duckdb::ClientContext &context) {
		return context.db->GetObjectCache();
	}

private:
	//! Object Cache
	std::unordered_map<std::string, shared_ptr<ObjectCacheEntry>> cache;
	std::mutex lock;
};

} // namespace duckdb
