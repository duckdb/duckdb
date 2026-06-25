#include "duckdb/main/feature_refresh_scheduler.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/feature_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/common/enums/access_mode.hpp"
#include "duckdb/common/exception/catalog_exception.hpp"
#include "duckdb/common/sql_identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/database.hpp"

#ifndef DUCKDB_NO_THREADS
#include <chrono>
#include <functional>
#endif

namespace duckdb {

#ifndef DUCKDB_NO_THREADS
class FeatureRefreshSchedulerTransactionState : public ClientContextState {
public:
	void NotifyOnCommit() {
		notify_on_commit = true;
	}

	void TransactionCommit(MetaTransaction &transaction, ClientContext &context) override {
		if (!notify_on_commit) {
			return;
		}
		notify_on_commit = false;
		DatabaseInstance::GetDatabase(context).NotifyFeatureRefreshScheduler();
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context) override {
		notify_on_commit = false;
	}

	void TransactionRollback(MetaTransaction &transaction, ClientContext &context,
	                         optional_ptr<ErrorData> error) override {
		notify_on_commit = false;
	}

private:
	bool notify_on_commit = false;
};
#endif

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
	// Like the TaskScheduler, the constructor only launches the background thread; it performs no
	// catalog work here. Doing a synchronous catalog scan in Start() is unsafe because Start() runs
	// inside the DuckDB constructor while the DBInstanceCache holds update_database_mutex, and a
	// concurrent opener of the same database busy-spins waiting on that path. The initial heap scan
	// therefore happens on the background thread (see Run()).
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

void FeatureRefreshScheduler::NotifyOnCommit(ClientContext &context) {
#ifndef DUCKDB_NO_THREADS
	auto state = context.registered_state->GetOrCreate<FeatureRefreshSchedulerTransactionState>(
	    "feature_refresh_scheduler_transaction_state");
	state->NotifyOnCommit();
#endif
}

void FeatureRefreshScheduler::RemoveCatalog(const string &catalog_name) {
#ifndef DUCKDB_NO_THREADS
	auto prefix = catalog_name + ".";
	bool removed = false;
	{
		lock_guard<std::mutex> lock(mtx);
		for (auto entry = known_next_refresh.begin(); entry != known_next_refresh.end();) {
			if (StringUtil::StartsWith(entry->first, prefix)) {
				entry = known_next_refresh.erase(entry);
				removed = true;
			} else {
				entry++;
			}
		}
	}
	if (removed) {
		heap_dirty.store(true);
		cv.notify_all();
	}
#endif
}

bool FeatureRefreshScheduler::GetNextRefreshAt(const string &catalog_name, const string &schema_name,
                                               const string &feature_name, timestamp_t &result) {
#ifndef DUCKDB_NO_THREADS
	lock_guard<std::mutex> lock(mtx);
	auto entry = known_next_refresh.find(MakeKey(catalog_name, schema_name, feature_name));
	if (entry == known_next_refresh.end()) {
		return false;
	}
	result = entry->second;
	return true;
#else
	return false;
#endif
}

string FeatureRefreshScheduler::MakeKey(const string &catalog_name, const string &schema_name,
                                        const string &feature_name) {
	return catalog_name + "." + schema_name + "." + feature_name;
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
	try {
		Connection con(db);
		con.BeginTransaction();
		auto now = Timestamp::GetCurrentTimestamp();

		unordered_map<string, timestamp_t> new_known;

		auto schemas = Catalog::GetAllSchemas(*con.context);
		for (auto &schema_ref : schemas) {
			schema_ref.get().Scan(*con.context, CatalogType::FEATURE_ENTRY, [&](CatalogEntry &raw_entry) {
				auto &feat = raw_entry.Cast<FeatureCatalogEntry>();
				if (feat.catalog.GetAttached().IsReadOnly()) {
					return;
				}
				if (!feat.has_schedule || !feat.schedule_enabled) {
					return;
				}
				// Reject sub-second intervals to prevent a busy-loop.
				if (IntervalToMicros(feat.schedule_interval) < Interval::MICROS_PER_SEC) {
					return;
				}

				string key = MakeKey(feat.catalog.GetName(), feat.schema.name, feat.name);

				timestamp_t next_at;
				bool has_known_next;
				{
					lock_guard<std::mutex> lock(mtx);
					auto it = known_next_refresh.find(key);
					has_known_next = it != known_next_refresh.end();
					if (has_known_next) {
						next_at = it->second;
					}
				}
				if (has_known_next) {
					// Preserve the existing in-memory schedule so ALTER/NOTIFY rebuilds don't
					// reset timers that are already running.
				} else {
					// New feature: derive from last_refresh_timestamp + interval. Add per-feature
					// startup jitter — capped at 10% of the interval (max 30 s) — to spread out
					// concurrent wakeups without adding significant latency for short intervals.
					int64_t interval_us = IntervalToMicros(feat.schedule_interval);
					int64_t jitter_cap = std::min(interval_us / 10, static_cast<int64_t>(30000000LL));
					int64_t jitter_us =
					    jitter_cap > 0
					        ? static_cast<int64_t>(std::hash<string> {}(feat.name) % static_cast<size_t>(jitter_cap))
					        : 0;
					next_at = Interval::Add(feat.last_refresh_timestamp, feat.schedule_interval);
					next_at.value += jitter_us;
					// If the derived time is already in the past, fire from now.
					if (next_at <= now) {
						next_at = timestamp_t(now.value + jitter_us);
					}
				}
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

		{
			lock_guard<std::mutex> lock(mtx);
			known_next_refresh = std::move(new_known);
		}
		con.Commit();
	} catch (std::exception &) {
		// Catalog scan failed (DB shutting down, etc.) — leave heap empty; the loop will retry or exit.
	}
}

bool FeatureRefreshScheduler::ValidateScheduledFeature(ScheduledFeature &feature) {
	Connection con(db);
	con.BeginTransaction();
	EntryLookupInfo lookup_info(CatalogType::FEATURE_ENTRY, feature.feature_name);
	auto entry = Catalog::GetEntry(*con.context, feature.catalog_name, feature.schema_name, lookup_info,
	                              OnEntryNotFound::RETURN_NULL);
	if (!entry) {
		con.Commit();
		return false;
	}
	auto &catalog_entry = entry->Cast<FeatureCatalogEntry>();
	if (catalog_entry.catalog.GetAttached().IsReadOnly()) {
		con.Commit();
		return false;
	}
	if (!catalog_entry.has_schedule || !catalog_entry.schedule_enabled) {
		con.Commit();
		return false;
	}
	if (IntervalToMicros(catalog_entry.schedule_interval) < Interval::MICROS_PER_SEC) {
		con.Commit();
		return false;
	}
	feature.schedule_interval = catalog_entry.schedule_interval;
	con.Commit();
	return true;
}

void FeatureRefreshScheduler::Run() {
	// Start idle, like a TaskScheduler worker with no tasks: do NOT scan the catalog here. A scan
	// would create a Connection + transaction and allocate buffer-managed memory on every database
	// open, racing with queries (and memory-accounting assertions) that run right after startup, even
	// for databases that have no scheduled features at all. Instead we wait for a Notify() — fired when
	// a scheduled FeatureCatalogEntry is created (at startup via WAL replay or at runtime via CREATE
	// FEATURE) — which sets heap_dirty and triggers the scan below. Databases without scheduled
	// features therefore never run a scan and never touch the buffer manager from this thread.
	FeatureHeap heap;

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
			auto wake_tp = std::chrono::system_clock::time_point(std::chrono::microseconds(next_wakeup.value));
			unique_lock<std::mutex> lock(mtx);
			cv.wait_until(lock, wake_tp, [this] { return stop_requested.load() || heap_dirty.load(); });
			continue;
		}

		// Fire all features whose time has come.
		while (!stop_requested.load() && !heap.empty() && heap.top().next_refresh_at <= now) {
			auto feat = heap.top();
			heap.pop();

			try {
				Connection con(db);
				if (!ValidateScheduledFeature(feat)) {
					lock_guard<std::mutex> lock(mtx);
					known_next_refresh.erase(feat.key);
					continue;
				}
				auto use_result =
				    con.Query("USE " + SQLIdentifier(feat.catalog_name) + "." + SQLIdentifier(feat.schema_name));
				if (use_result->HasError()) {
					throw Exception(ExceptionType::IO, use_result->GetError());
				}
				auto result = con.Query("SELECT * FROM refresh_feature(" + SQLString(feat.feature_name) + ")");
				if (result->HasError()) {
					throw Exception(ExceptionType::IO, result->GetError());
				}
				feat.next_refresh_at = Interval::Add(now, feat.schedule_interval);
			} catch (CatalogException &) {
				// Feature was dropped — remove it from the scheduler entirely.
				{
					lock_guard<std::mutex> lock(mtx);
					known_next_refresh.erase(feat.key);
				}
				continue;
			} catch (std::exception &) {
				// Transient error — back off for min(interval, 5 minutes) before retrying.
				static constexpr int64_t FIVE_MIN_MICROS = 5LL * 60 * 1000000;
				int64_t backoff_us = std::min(IntervalToMicros(feat.schedule_interval), FIVE_MIN_MICROS);
				feat.next_refresh_at = timestamp_t(now.value + backoff_us);
			}

			{
				lock_guard<std::mutex> lock(mtx);
				known_next_refresh[feat.key] = feat.next_refresh_at;
			}
			heap.push(feat);
		}
	}
}

#endif // DUCKDB_NO_THREADS

} // namespace duckdb
