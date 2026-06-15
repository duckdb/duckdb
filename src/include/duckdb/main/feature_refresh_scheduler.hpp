//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/feature_refresh_scheduler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"

#ifndef DUCKDB_NO_THREADS
#include <atomic>
#include <condition_variable>
#include <queue>
#include <thread>
#endif

namespace duckdb {

class DatabaseInstance;

//! Owns a background thread that fires REFRESH FEATURE on each scheduled feature at its configured interval.
//! Disabled for READ_ONLY databases and DUCKDB_NO_THREADS builds.
class FeatureRefreshScheduler {
public:
	explicit FeatureRefreshScheduler(DatabaseInstance &db);
	~FeatureRefreshScheduler();

	//! Start the background thread. No-op for READ_ONLY or no-threads builds.
	void Start();
	//! Signal the thread to stop and join it. Must be called before catalog teardown.
	void Stop();
	//! Wake the scheduler so it re-scans the catalog (call after CREATE/ALTER/DROP changes a schedule).
	void Notify();

private:
	struct ScheduledFeature {
		timestamp_t next_refresh_at;
		string feature_name;
		string catalog_name;
		string schema_name;
		//! Stable key used to preserve next_refresh_at across heap rebuilds.
		string key;
		interval_t schedule_interval;
	};

	struct FeatureCmp {
		bool operator()(const ScheduledFeature &a, const ScheduledFeature &b) const {
			return a.next_refresh_at > b.next_refresh_at; // min-heap: soonest at top
		}
	};

#ifndef DUCKDB_NO_THREADS
	using FeatureHeap = std::priority_queue<ScheduledFeature, vector<ScheduledFeature>, FeatureCmp>;

	void Run();
	//! Scan the catalog for scheduled features and (re)build the heap. Preserves next_refresh_at for
	//! features already tracked so heap rebuilds don't reset in-flight timers.
	void RebuildHeap(FeatureHeap &heap);
#endif

	//! Convert an interval_t to an approximate microsecond count (1 month ≈ 30 days).
	static int64_t IntervalToMicros(const interval_t &interval);

	DatabaseInstance &db;
	//! Remembers each feature's next_refresh_at between heap rebuilds.
	unordered_map<string, timestamp_t> known_next_refresh;

#ifndef DUCKDB_NO_THREADS
	std::thread bg_thread;
	std::mutex mtx;
	std::condition_variable cv;
	std::atomic<bool> stop_requested {false};
	std::atomic<bool> heap_dirty {false};
	bool started = false;
#endif
};

} // namespace duckdb
