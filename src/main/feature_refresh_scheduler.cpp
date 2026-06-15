#include "duckdb/main/feature_refresh_scheduler.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/connection.hpp"
#include <cstdio>
#include "duckdb/main/database.hpp"

#ifndef DUCKDB_NO_THREADS
#include <chrono>
#include <functional>
#endif

namespace duckdb {

FeatureRefreshScheduler::FeatureRefreshScheduler(DatabaseInstance &db_p) : db(db_p) {
}

FeatureRefreshScheduler::~FeatureRefreshScheduler() {
	Stop();
}

void FeatureRefreshScheduler::Start() {
#ifndef DUCKDB_NO_THREADS
	if (db.config.options.access_mode == AccessMode::READ_ONLY) {
		return;
	}
	if (started) {
		return;
	}
	started = true;
	stop_requested.store(false);
	bg_thread = std::thread([this] { Run(); });
#endif
}

void FeatureRefreshScheduler::Stop() {
#ifndef DUCKDB_NO_THREADS
	if (!started) {
		return;
	}
	started = false;
	stop_requested.store(true);
	cv.notify_all();
	if (bg_thread.joinable()) {
		bg_thread.join();
	}
#endif
}

void FeatureRefreshScheduler::Notify() {
#ifndef DUCKDB_NO_THREADS
	heap_dirty.store(true);
	cv.notify_all();
#endif
}

int64_t FeatureRefreshScheduler::IntervalToMicros(const interval_t &interval) {
	// 1 month ≈ 30 days — good enough for scheduling purposes.
	return interval.micros + static_cast<int64_t>(interval.days) * Interval::MICROS_PER_DAY +
	       static_cast<int64_t>(interval.months) * Interval::MICROS_PER_MONTH;
}

#ifndef DUCKDB_NO_THREADS

void FeatureRefreshScheduler::RebuildHeap(FeatureHeap &heap) {
	while (!heap.empty()) {
		heap.pop();
	}
	fprintf(stderr, "[scheduler] RebuildHeap called\n");
	try {
		Connection con(db);
		auto now = Timestamp::GetCurrentTimestamp();

		unordered_map<string, timestamp_t> new_known;

		auto schemas = Catalog::GetAllSchemas(*con.context);
		for (auto &schema_ref : schemas) {
			schema_ref.get().Scan(*con.context, CatalogType::FEATURE_ENTRY, [&](CatalogEntry &raw_entry) {
				auto &feat = raw_entry.Cast<FeatureCatalogEntry>();
				fprintf(stderr, "[scheduler] found feature '%s' has_schedule=%d schedule_enabled=%d interval_us=%lld\n",
				        feat.name.c_str(), (int)feat.has_schedule, (int)feat.schedule_enabled,
				        (long long)IntervalToMicros(feat.schedule_interval));
				if (!feat.has_schedule || !feat.schedule_enabled) {
					return;
				}
				// Reject sub-second intervals to prevent a busy-loop.
				if (IntervalToMicros(feat.schedule_interval) < Interval::MICROS_PER_SEC) {
					fprintf(stderr, "[scheduler] feature '%s' rejected: sub-second interval\n", feat.name.c_str());
					return;
				}

				string key = feat.catalog.GetName() + "." + feat.schema.name + "." + feat.name;

				timestamp_t next_at;
				auto it = known_next_refresh.find(key);
				if (it != known_next_refresh.end()) {
					// Preserve the existing in-memory schedule so ALTER/NOTIFY rebuilds don't
					// reset timers that are already running.
					next_at = it->second;
				} else {
					// New feature: derive from last_refresh_timestamp + interval. Add per-feature
					// startup jitter — capped at 10% of the interval (max 30 s) — to spread out
					// concurrent wakeups without adding significant latency for short intervals.
					int64_t interval_us = IntervalToMicros(feat.schedule_interval);
					int64_t jitter_cap = std::min(interval_us / 10, static_cast<int64_t>(30000000LL));
					int64_t jitter_us = jitter_cap > 0
					                        ? static_cast<int64_t>(std::hash<string> {}(feat.name) %
					                                               static_cast<size_t>(jitter_cap))
					                        : 0;
					next_at = Interval::Add(feat.last_refresh_timestamp, feat.schedule_interval);
					next_at.value += jitter_us;
					// If the derived time is already in the past, fire from now.
					if (next_at <= now) {
						next_at = timestamp_t(now.value + jitter_us);
					}
				}
				fprintf(stderr, "[scheduler] scheduling '%s' next_at_delta_us=%lld\n",
				        feat.name.c_str(), (long long)(next_at.value - now.value));
				new_known[key] = next_at;

				ScheduledFeature sf;
				sf.next_refresh_at = next_at;
				sf.feature_name = feat.name;
				sf.catalog_name = feat.catalog.GetName();
				sf.schema_name = feat.schema.name;
				sf.key = key;
				sf.schedule_interval = feat.schedule_interval;
				heap.push(sf);
			});
		}

		known_next_refresh = std::move(new_known);
	} catch (std::exception &ex) {
		// Catalog scan failed (DB shutting down, etc.) — leave heap empty; the loop will retry or exit.
		fprintf(stderr, "[scheduler] RebuildHeap exception: %s\n", ex.what());
	}
	fprintf(stderr, "[scheduler] RebuildHeap done, heap size=%zu\n", heap.size());
}

void FeatureRefreshScheduler::Run() {
	FeatureHeap heap;
	RebuildHeap(heap);

	while (!stop_requested.load()) {
		// Honor a pending re-scan request from Notify().
		if (heap_dirty.exchange(false)) {
			RebuildHeap(heap);
		}

		if (heap.empty()) {
			// Nothing scheduled — block until notified or stopped.
			unique_lock<std::mutex> lock(mtx);
			cv.wait(lock, [this] { return stop_requested.load() || heap_dirty.load(); });
			continue;
		}

		auto now = Timestamp::GetCurrentTimestamp();
		auto next_wakeup = heap.top().next_refresh_at;

		if (now < next_wakeup) {
			// Sleep until the next feature is due, waking early on Notify() or Stop().
			auto wake_tp =
			    std::chrono::system_clock::time_point(std::chrono::microseconds(next_wakeup.value));
			unique_lock<std::mutex> lock(mtx);
			cv.wait_until(lock, wake_tp,
			              [this] { return stop_requested.load() || heap_dirty.load(); });
			continue;
		}

		// Fire all features whose time has come.
		while (!stop_requested.load() && !heap.empty() && heap.top().next_refresh_at <= now) {
			auto feat = heap.top();
			heap.pop();
			fprintf(stderr, "[scheduler] firing refresh_feature('%s')\n", feat.feature_name.c_str());

			try {
				Connection con(db);
				auto result = con.Query("SELECT * FROM refresh_feature('" + feat.feature_name + "')");
				if (result->HasError()) {
					fprintf(stderr, "[scheduler] refresh_feature('%s') result error: %s\n",
					        feat.feature_name.c_str(), result->GetError().c_str());
					throw Exception(ExceptionType::IO, result->GetError());
				}
				fprintf(stderr, "[scheduler] refresh_feature('%s') succeeded\n", feat.feature_name.c_str());
				feat.next_refresh_at = Interval::Add(now, feat.schedule_interval);
			} catch (CatalogException &ex) {
				// Feature was dropped — remove it from the scheduler entirely.
				fprintf(stderr, "[scheduler] refresh_feature('%s') CatalogException (dropped?): %s\n",
				        feat.feature_name.c_str(), ex.what());
				known_next_refresh.erase(feat.key);
				continue;
			} catch (std::exception &ex) {
				// Transient error — back off for min(interval, 5 minutes) before retrying.
				fprintf(stderr, "[scheduler] refresh_feature('%s') exception: %s\n",
				        feat.feature_name.c_str(), ex.what());
				static constexpr int64_t FIVE_MIN_MICROS = 5LL * 60 * 1000000;
				int64_t backoff_us =
				    std::min(IntervalToMicros(feat.schedule_interval), FIVE_MIN_MICROS);
				feat.next_refresh_at = timestamp_t(now.value + backoff_us);
			}

			known_next_refresh[feat.key] = feat.next_refresh_at;
			heap.push(feat);
		}
	}
}

#endif // DUCKDB_NO_THREADS

} // namespace duckdb
