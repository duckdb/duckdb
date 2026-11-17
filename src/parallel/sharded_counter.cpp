#include "duckdb/parallel/sharded_counter.hpp"

#include "duckdb/parallel/task_scheduler.hpp"

namespace duckdb {

void ShardedCounter::Add(idx_t count) {
	const auto shard = (TaskScheduler::GetEstimatedCPUId() / CPUS_PER_SHARD) % SHARD_COUNT;
	shards[shard].fetch_add(count, std::memory_order_relaxed);
}

idx_t ShardedCounter::GetTotal() const {
	idx_t total = 0;
	for (const auto &shard : shards) {
		total += shard.load(std::memory_order_relaxed);
	}
	return total;
}

void ShardedCounter::Reset() {
	for (auto &shard : shards) {
		shard.store(0, std::memory_order_relaxed);
	}
}

} // namespace duckdb
