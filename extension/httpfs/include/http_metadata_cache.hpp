#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <stddef.h>
#include <string>


namespace duckdb {

struct HTTPMetadataCacheEntry {
	string path;
	idx_t length;
	time_t last_modified;
	milliseconds cache_time;
};

// Simple cache with a max age for an entry to be valid
class HTTPMetadataCache {
public:
	//! Insert entry in cache
	void Insert(string path, idx_t length, time_t last_modified);
	//! Erase entry from cache
	void Erase(string path);
	//! Find entry if its age < within max_age, if entry was found but too old: delete it
	bool Find(string path, HTTPMetadataCacheEntry &ret_val, uint64_t max_age);
	//! Prune all entries older than max_age
	void PruneExpired(uint64_t max_age);
	//! Clear the whole cache
	void Clear();

protected:
	mutex lock;
	unordered_map<string, list<HTTPMetadataCacheEntry>::iterator> map;
	list<HTTPMetadataCacheEntry> list;
	atomic<bool> empty {true};

	void EraseInternal(string path);
};

} // namespace duckdb
