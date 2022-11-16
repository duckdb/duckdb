#include "http_metadata_cache.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/types.hpp"

namespace duckdb {

void HTTPMetadataCache::Insert(string path, idx_t length, time_t last_modified) {
	lock_guard<mutex> parallel_lock(lock);
	empty = false;
	milliseconds current_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
	list.push_back({path, length, last_modified, current_time});
	map[path] = --list.end();
}

void HTTPMetadataCache::Erase(string path) {
	lock_guard<mutex> parallel_lock(lock);
	EraseInternal(path);
}

bool HTTPMetadataCache::Find(string path, HTTPMetadataCacheEntry &ret_val, uint64_t max_age) {
	lock_guard<mutex> parallel_lock(lock);
	auto lookup = map.find(path);
	bool found = lookup != map.end();
	if (found) {
		milliseconds current_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch());
		if (current_time - lookup->second->cache_time <= milliseconds(max_age)) {
			ret_val = *lookup->second;
			return true;
		}
		EraseInternal(path);
	}

	return false;
}

void HTTPMetadataCache::PruneExpired(uint64_t max_age) {
	lock_guard<mutex> parallel_lock(lock);
	milliseconds current_time = duration_cast<milliseconds>(system_clock::now().time_since_epoch());

	auto it = list.begin();
	while (it != list.end()) {
		if (current_time - it->cache_time > milliseconds(max_age)) {
			map.erase(it->path);
			it = list.erase(it);
		} else {
			return;
		}
	}

	empty = true;
}

void HTTPMetadataCache::Clear() {
	if (empty) {
		return;
	}
	lock_guard<mutex> parallel_lock(lock);
	list.clear();
	map.clear();
	empty = true;
}

void HTTPMetadataCache::EraseInternal(string path) {
	auto lookup = map.find(path);
	if (lookup != map.end()) {
		list.erase(lookup->second);
	}
	map.erase(path);
}

} // namespace duckdb
