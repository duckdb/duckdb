//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parallel/scan_affinity_queue.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/atomic.hpp"
#include "duckdb/common/common.hpp"
#include "duckdb/common/deque.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class ScanAffinityQueueTestHelper;
struct ScanReadAheadJob;

//! Schedules ready scan jobs by an optional shard hint, independently of their reader
class ScanAffinityQueue {
	friend class ScanAffinityQueueTestHelper;

public:
	explicit ScanAffinityQueue(idx_t shard_count);
	~ScanAffinityQueue();

	//! Publish ready work with an optional placement hint.
	void Push(idx_t producer_shard, optional_idx affinity, unique_ptr<ScanReadAheadJob> job);

	//! Prefer nearby cached work, then unknown placement. Empty hints are advisory only.
	bool TryPop(idx_t local_shard, unique_ptr<ScanReadAheadJob> &job);

private:
	enum class ProbeMode { ADVISORY, AUTHORITATIVE };

	struct AssignmentQueue {
		deque<unique_ptr<ScanReadAheadJob>> assignments;
		//! Advisory count used to skip empty queues without taking their lock
		atomic<idx_t> ready {0};
	};

	struct alignas(128) Shard {
		explicit Shard(idx_t probe_index);

		mutex lock;
		AssignmentQueue affinity;
		AssignmentQueue unknown;
		const idx_t probe_index;
	};

	bool TryTakeAssignment(idx_t probe_index, ProbeMode mode, unique_ptr<ScanReadAheadJob> &job);

private:
	vector<unique_ptr<Shard>> shards;
	vector<optional_ptr<Shard>> probe_slots;
};

} // namespace duckdb
