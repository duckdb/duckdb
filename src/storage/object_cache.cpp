#include "duckdb/storage/object_cache.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/buffer/buffer_pool.hpp"

namespace duckdb {

ObjectCache &ObjectCache::GetObjectCache(ClientContext &context) {
	return context.db->GetObjectCache();
}

shared_ptr<ObjectCacheEntry>
ObjectCache::GetOrCreateObject(const string &key, const std::function<shared_ptr<ObjectCacheEntry>()> &create) {
	while (true) {
		auto existing = GetObject(key);
		if (existing) {
			return existing;
		}
		shared_ptr<InFlightCreation> creation;
		bool is_creator = false;
		{
			const lock_guard<mutex> guard(in_flight_lock);
			auto &slot = in_flight_creations[key];
			if (!slot) {
				slot = make_shared_ptr<InFlightCreation>();
				is_creator = true;
			}
			creation = slot;
		}
		if (!is_creator) {
			unique_lock<mutex> guard(creation->lock);
			creation->finished_cv.wait(guard, [&]() { return creation->finished; });
			// look up again: the creator may have failed, or the entry may already be gone
			continue;
		}
		struct FinishOnExit {
			ObjectCache &cache;
			const string &key;
			InFlightCreation &creation;
			~FinishOnExit() {
				cache.FinishCreation(key, creation);
			}
		} finish_on_exit {*this, key, *creation};
		// a creation that finished between the lookup above and our registration
		existing = GetObject(key);
		if (existing) {
			return existing;
		}
		auto created = create();
		if (!created) {
			return nullptr;
		}
		return GetOrPut(key, std::move(created));
	}
}

void ObjectCache::FinishCreation(const string &key, InFlightCreation &creation) {
	{
		const lock_guard<mutex> guard(in_flight_lock);
		in_flight_creations.erase(key);
	}
	{
		const lock_guard<mutex> guard(creation.lock);
		creation.finished = true;
	}
	creation.finished_cv.notify_all();
}

bool ObjectCache::EvictToMemoryLimit(idx_t memory_limit) {
	constexpr idx_t EVICTION_BATCH_SIZE = 16;
	while (true) {
		// Release the cache lock between batches to let lookups make progress.
		const lock_guard<mutex> lock(lock_mutex);
		for (idx_t i = 0; i < EVICTION_BATCH_SIZE; i++) {
			// Entries can own reservations beyond their cache weight, or remain alive after eviction.
			if (buffer_pool.GetUsedMemory() <= memory_limit) {
				return true;
			}
			if (!lru_cache.EvictLeastRecentlyUsed()) {
				return false;
			}
		}
	}
}

} // namespace duckdb
