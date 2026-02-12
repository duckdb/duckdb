//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/lru_cache.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <functional>

#include "duckdb/common/common.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/list.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/shared_ptr.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/storage/buffer/buffer_pool_reservation.hpp"

namespace duckdb {

struct DefaultPayload {
	idx_t GetWeight() const {
		return 1;
	}
};

// A LRU cache implementation, whose value could be accessed in a shared manner with shared pointer.
// Notice, it's not thread-safe.
template <typename Key, typename Val, typename Payload = DefaultPayload, typename KeyHash = std::hash<Key>,
          typename KeyEqual = std::equal_to<Key>>
class SharedLruCache {
public:
	using key_type = Key;
	using mapped_type = shared_ptr<Val>;
	using hasher = KeyHash;
	using key_equal = KeyEqual;

	// @param max_total_weight_p: Maximum total weight (in relevant unit) of entries. 0 means unlimited.
	explicit SharedLruCache(idx_t max_total_weight_p) : max_total_weight(max_total_weight_p), current_total_weight(0) {
	}

	// Disable copy and move
	SharedLruCache(const SharedLruCache &) = delete;
	SharedLruCache &operator=(const SharedLruCache &) = delete;

	~SharedLruCache() = default;

	// Insert `value` with key `key` and explicit memory size. This will replace any previous entry with the same key.
	template <typename... Types>
	void Put(Key key, shared_ptr<Val> value, Types... constructor_args) {
		// Remove existing entry if present
		auto existing_it = entry_map.find(key);
		if (existing_it != entry_map.end()) {
			DeleteImpl(existing_it);
		}

		auto payload = make_uniq<Payload>(std::forward<Types>(constructor_args)...);
		auto payload_weight = payload->GetWeight();

		// Evict entries if needed to make room
		if (max_total_weight > 0 && payload_weight > 0) {
			EvictIfNeeded(payload_weight);
		}

		// Add new entry
		lru_list.emplace_front(key);
		Entry new_entry;
		new_entry.value = std::move(value);
		new_entry.lru_iterator = lru_list.begin();
		new_entry.payload = std::move(payload);
		new_entry.payload_weight = payload_weight;

		entry_map[std::move(key)] = std::move(new_entry);
		current_total_weight += payload_weight;
	}

	// Delete the entry with key `key`.
	// Return whether the requested `key` is found in the cache.
	bool Delete(const Key &key) {
		auto it = entry_map.find(key);
		if (it == entry_map.end()) {
			return false;
		}
		DeleteImpl(it);
		return true;
	}

	// Look up the entry with key `key`. Return nullptr if not found.
	shared_ptr<Val> Get(const Key &key) {
		auto entry_map_iter = entry_map.find(key);
		if (entry_map_iter == entry_map.end()) {
			return nullptr;
		}

		// Move to front, which indicates most recently used.
		lru_list.splice(lru_list.begin(), lru_list, entry_map_iter->second.lru_iterator);
		return entry_map_iter->second.value;
	}

	// Clear the whole cache.
	void Clear() {
		entry_map.clear();
		lru_list.clear();
		current_total_weight = 0;
	}

	// Evict entries based on their access, until we've freed at least the target number of bytes or there's no entries
	// in the cache. Return number of bytes freed.
	idx_t EvictToReduceAtLeast(idx_t target_weight) {
		idx_t freed = 0;
		while (!lru_list.empty() && freed < target_weight) {
			const auto &stale_key = lru_list.back();
			auto stale_it = entry_map.find(stale_key);
			D_ASSERT(stale_it != entry_map.end());
			freed += stale_it->second.payload_weight;
			DeleteImpl(stale_it);
		}
		return freed;
	}

	idx_t Capacity() const {
		return max_total_weight;
	}
	idx_t CurrentTotalWeight() const {
		return current_total_weight;
	}
	size_t Size() const {
		return entry_map.size();
	}
	bool IsEmpty() const {
		return entry_map.empty();
	}

private:
	struct Entry {
		shared_ptr<Val> value;
		idx_t memory;
		typename list<Key>::iterator lru_iterator;
		// Record payload weight, which is used for global weight control.
		unique_ptr<Payload> payload;
		idx_t payload_weight;
	};

	using EntryMap = unordered_map<Key, Entry, KeyHash, KeyEqual>;

	void DeleteImpl(typename EntryMap::iterator iter) {
		current_total_weight -= iter->second.payload_weight;
		D_ASSERT(current_total_weight >= 0);
		lru_list.erase(iter->second.lru_iterator);
		entry_map.erase(iter);
	}

	void EvictIfNeeded(idx_t required_weight) {
		if (max_total_weight == 0) {
			return;
		}

		// Evict LRU entries until we have enough space
		while (!lru_list.empty() && (current_total_weight + required_weight > max_total_weight)) {
			const auto &stale_key = lru_list.back();
			auto stale_it = entry_map.find(stale_key);
			if (stale_it != entry_map.end()) {
				DeleteImpl(stale_it);
			} else {
				// Should not happen, but be defensive
				lru_list.pop_back();
			}
		}
	}

	const idx_t max_total_weight;
	idx_t current_total_weight;
	EntryMap entry_map;
	list<Key> lru_list;
};

} // namespace duckdb
