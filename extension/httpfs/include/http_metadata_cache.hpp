#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/main/client_context.hpp"

#include <stddef.h>
#include <string>

namespace duckdb {

struct HTTPMetadataCacheEntry {
	idx_t length;
	time_t last_modified;
};

// Simple cache with a max age for an entry to be valid
class HTTPMetadataCache : public ClientContextState {
public:
	explicit HTTPMetadataCache(bool flush_on_query_end_p, bool shared_p)
	    : flush_on_query_end(flush_on_query_end_p), shared(shared_p) {};

	void Insert(const string &path, HTTPMetadataCacheEntry val) {
		if (shared) {
			lock_guard<mutex> parallel_lock(lock);
			map[path] = val;
		} else {
			map[path] = val;
		}
	};

	void Erase(string path) {
		if (shared) {
			lock_guard<mutex> parallel_lock(lock);
			map.erase(path);
		} else {
			map.erase(path);
		}
	};

	bool Find(string path, HTTPMetadataCacheEntry &ret_val) {
		if (shared) {
			lock_guard<mutex> parallel_lock(lock);
			auto lookup = map.find(path);
			if (lookup != map.end()) {
				ret_val = lookup->second;
				return true;
			} else {
				return false;
			}
		} else {
			auto lookup = map.find(path);
			if (lookup != map.end()) {
				ret_val = lookup->second;
				return true;
			} else {
				return false;
			}
		}
	};

	void Clear() {
		if (shared) {
			lock_guard<mutex> parallel_lock(lock);
			map.clear();
		} else {
			map.clear();
		}
	}

	//! Called by the ClientContext when the current query ends
	void QueryEnd() override {
		if (flush_on_query_end) {
			Clear();
		}
	}

protected:
	mutex lock;
	unordered_map<string, HTTPMetadataCacheEntry> map;
	bool flush_on_query_end;
	bool shared;
};

} // namespace duckdb
