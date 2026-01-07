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

// A LRU cache implementation, whose value could be accessed in a shared manner with shared pointer.
// Notice, it's not thread-safe.
template <typename Key, typename Val, typename KeyHash = std::hash<Key>, typename KeyEqual = std::equal_to<Key>>
class SharedLruCache {
public:
	using key_type = Key;
	using mapped_type = shared_ptr<Val>;
	using hasher = KeyHash;
	using key_equal = KeyEqual;

	// @param max_memory_p: Maximum total memory (in bytes) of entries. 0 means unlimited.
	explicit SharedLruCache(idx_t max_memory_p) : max_memory(max_memory_p), current_memory(0) {
	}

	// Disable copy and move
	SharedLruCache(const SharedLruCache &) = delete;
	SharedLruCache &operator=(const SharedLruCache &) = delete;

	~SharedLruCache() = default;

	// Insert `value` with key `key` and explicit memory size. This will replace any previous entry with the same key.
	void Put(Key key, shared_ptr<Val> value, unique_ptr<BufferPoolReservation> reservation) {
		if (reservation == nullptr) {
			throw InvalidInputException("Reservation cannot be null when emplace into LRU!");
		}

		// Remove existing entry if present
		const idx_t memory_size = reservation->size;
		auto existing_it = entry_map.find(key);
		if (existing_it != entry_map.end()) {
			DeleteImpl(existing_it);
		}

		// Evict entries if needed to make room
		if (max_memory > 0 && memory_size > 0) {
			EvictIfNeeded(memory_size);
		}

		// Add new entry
		lru_list.emplace_front(key);
		Entry new_entry;
		new_entry.value = std::move(value);
		new_entry.lru_iterator = lru_list.begin();
		new_entry.reservation = std::move(reservation);

		entry_map[std::move(key)] = std::move(new_entry);
		current_memory += memory_size;
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
		for (auto &entry : entry_map) {
			D_ASSERT(entry.second.reservation != nullptr);
			entry.second.reservation->Resize(0);
		}
		entry_map.clear();
		lru_list.clear();
		current_memory = 0;
	}

	// Evict entries based on their access, until we've freed at least the target number of bytes or there's no entries in the cache.
	// Return number of bytes freed.
	idx_t EvictToReduceMemory(idx_t target_bytes) {
		idx_t freed = 0;
		while (!lru_list.empty() && freed < target_bytes) {
			const auto &stale_key = lru_list.back();
			auto stale_it = entry_map.find(stale_key);
			D_ASSERT(stale_it != entry_map.end());
			freed += stale_it->second.reservation->size;
			DeleteImpl(stale_it);
		}
		return freed;
	}

	idx_t MaxMemory() const {
		return max_memory;
	}
	idx_t CurrentMemory() const {
		return current_memory;
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
		// Record memory reservation in the buffer pool, which is used for global memory control.
		unique_ptr<BufferPoolReservation> reservation;
	};

	using EntryMap = unordered_map<Key, Entry, KeyHash, KeyEqual>;

	void DeleteImpl(typename EntryMap::iterator iter) {
		current_memory -= iter->second.reservation->size;
		D_ASSERT(current_memory >= 0);
		iter->second.reservation->Resize(0);
		lru_list.erase(iter->second.lru_iterator);
		entry_map.erase(iter);
	}

	void EvictIfNeeded(idx_t required_memory) {
		if (max_memory == 0) {
			return;
		}

		// Evict LRU entries until we have enough space
		while (!lru_list.empty() && (current_memory + required_memory > max_memory)) {
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

	const idx_t max_memory;
	idx_t current_memory;
	EntryMap entry_map;
	list<Key> lru_list;
};

} // namespace duckdb
