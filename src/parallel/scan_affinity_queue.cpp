#include "duckdb/parallel/scan_affinity_queue.hpp"

#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/parallel/scan_read_ahead.hpp"

namespace duckdb {

ScanAffinityQueue::Shard::Shard(idx_t probe_index) : probe_index(probe_index) {
}

ScanAffinityQueue::ScanAffinityQueue(idx_t shard_count) : probe_slots(NextPowerOfTwo(shard_count)) {
	D_ASSERT(shard_count > 0);
	// Spread shards across power-of-two slots so XOR probing keeps balanced ranges together.
	for (idx_t i = 0; i < shard_count; i++) {
		const auto probe_index = i * probe_slots.size() / shard_count;
		shards.push_back(make_uniq<Shard>(probe_index));
		probe_slots[probe_index] = shards.back().get();
	}
}

ScanAffinityQueue::~ScanAffinityQueue() = default;

void ScanAffinityQueue::Push(idx_t producer_shard, optional_idx affinity, unique_ptr<ScanReadAheadJob> job) {
	const auto destination = affinity.IsValid() ? affinity.GetIndex() : producer_shard;
	D_ASSERT(destination < shards.size());
	auto &shard = *shards[destination];
	lock_guard<mutex> guard(shard.lock);
	auto &queue = affinity.IsValid() ? shard.affinity : shard.unknown;
	queue.assignments.push_back(std::move(job));
	queue.ready.store(queue.assignments.size(), std::memory_order_relaxed);
}

bool ScanAffinityQueue::TryPop(idx_t local_shard, unique_ptr<ScanReadAheadJob> &job) {
	D_ASSERT(local_shard < shards.size());
	const auto probe_index = shards[local_shard]->probe_index;
	return TryTakeAssignment(probe_index, ProbeMode::ADVISORY, job) ||
	       TryTakeAssignment(probe_index, ProbeMode::AUTHORITATIVE, job);
}

bool ScanAffinityQueue::TryTakeAssignment(idx_t probe_index, ProbeMode mode, unique_ptr<ScanReadAheadJob> &job) {
	// Prefer cached assignments before considering unknown placement.
	for (const auto has_affinity : {true, false}) {
		for (idx_t distance = 0; distance < probe_slots.size(); distance++) {
			auto slot = probe_slots[probe_index ^ distance];
			if (!slot) {
				continue;
			}
			auto &shard = *slot;
			auto &queue = has_affinity ? shard.affinity : shard.unknown;
			if (mode == ProbeMode::ADVISORY && !queue.ready.load(std::memory_order_relaxed)) {
				continue;
			}
			{
				lock_guard<mutex> guard(shard.lock);
				if (queue.assignments.empty()) {
					continue;
				}
				job = std::move(queue.assignments.front());
				queue.assignments.pop_front();
				queue.ready.store(queue.assignments.size(), std::memory_order_relaxed);
			}
			return true;
		}
	}
	return false;
}

} // namespace duckdb
