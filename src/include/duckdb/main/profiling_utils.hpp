//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// duckdb/main/profiling_utils.hpp
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/enums/metric_type.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/main/gathered_metrics.hpp"
#include "duckdb/main/profiling_node.hpp"
#include "duckdb/common/profiler.hpp"

namespace duckdb_yyjson {
struct yyjson_mut_doc;
struct yyjson_mut_val;
} // namespace duckdb_yyjson

namespace duckdb {

struct MetricsTimer;

// Top level query metrics
struct QueryMetrics {
public:
	QueryMetrics();
	~QueryMetrics();

	idx_t system_peak_buffer_memory;
	idx_t system_peak_temp_dir_size;
	double blocked_thread_time;

	std::string query_sql;

	// Always-tracked byte counters (used by progress bar even when profiling is disabled)
	atomic<idx_t> bytes_read;
	atomic<idx_t> bytes_written;
	// Thread-safe memory allocation counter (updated from allocator callbacks on any thread)
	atomic<idx_t> total_memory_allocated;

public:
	void UpdateMetric(const string &key, idx_t addition) {
		string_timings[key] += addition;
	}

	void UpdateMetricCounter(const string &key, idx_t addition) {
		string_counters[key] += addition;
	}

	void UpdateBytesRead(idx_t n) {
		bytes_read += n;
	}

	void UpdateBytesWritten(idx_t n) {
		bytes_written += n;
	}

	void UpdateTotalMemoryAllocated(idx_t n) {
		total_memory_allocated += n;
	}

	double GetStringMetricInSeconds(const string &key) const {
		auto it = string_timings.find(key);
		if (it == string_timings.end()) {
			return 0.0;
		}
		return static_cast<double>(it->second) / 1e9;
	}

	idx_t GetStringCounter(const string &key) const {
		auto it = string_counters.find(key);
		if (it == string_counters.end()) {
			return 0;
		}
		return it->second;
	}

	idx_t GetBytesRead() const {
		return bytes_read.load();
	}

	idx_t GetBytesWritten() const {
		return bytes_written.load();
	}

	idx_t GetTotalMemoryAllocated() const {
		return total_memory_allocated.load();
	}

	const unordered_map<string, idx_t> &GetMetricTimings() const {
		return string_timings;
	}

	const unordered_map<string, idx_t> &GetMetricCounters() const {
		return string_counters;
	}

	void Reset() {
		string_timings.clear();
		string_counters.clear();
		bytes_read = 0;
		bytes_written = 0;
		total_memory_allocated = 0;

		latency_timer.reset();
		query_sql = "";
		system_peak_buffer_memory = 0;
		system_peak_temp_dir_size = 0;
		blocked_thread_time = 0;
	}

	//! Write all query-level metrics into the given GatheredMetrics.
	void FinalizeMetrics(GatheredMetrics &info);

	void Merge(const QueryMetrics &other) {
		for (const auto &entry : other.string_timings) {
			string_timings[entry.first] += entry.second;
		}
		for (const auto &entry : other.string_counters) {
			string_counters[entry.first] += entry.second;
		}
		bytes_read += other.bytes_read.load();
		bytes_written += other.bytes_written.load();
		total_memory_allocated += other.total_memory_allocated.load();
	}

private:
	// String-keyed timings for optimizer, storage and phase timing metrics
	unordered_map<string, idx_t> string_timings;
	// String-keyed counters for storage counter metrics (e.g. "storage.wal_replay_entry_count")
	unordered_map<string, idx_t> string_counters;

public:
	// Declared after string_timings so it is destroyed first; its destructor writes to string_timings.
	unique_ptr<MetricsTimer> latency_timer;
};

struct MetricsTimer {
public:
	MetricsTimer() : metric_name(""), is_active(false) {
	}
	MetricsTimer(QueryMetrics &query_metrics, string key, const bool is_active = true)
	    : query_metrics(query_metrics), metric_name(std::move(key)), is_active(is_active) {
		if (!is_active) {
			return;
		}
		profiler.Start();
	}
	~MetricsTimer() {
		if (is_active && !Exception::UncaughtException()) {
			EndTimer();
		}
	}
	// disable copy constructors
	MetricsTimer(const MetricsTimer &other) = delete;
	MetricsTimer &operator=(const MetricsTimer &) = delete;
	//! enable move constructors
	MetricsTimer(MetricsTimer &&other) noexcept : is_active(false) {
		std::swap(query_metrics, other.query_metrics);
		std::swap(metric_name, other.metric_name);
		std::swap(profiler, other.profiler);
		std::swap(is_active, other.is_active);
	}
	MetricsTimer &operator=(MetricsTimer &&other) noexcept {
		std::swap(query_metrics, other.query_metrics);
		std::swap(metric_name, other.metric_name);
		std::swap(profiler, other.profiler);
		std::swap(is_active, other.is_active);
		return *this;
	}

	// Automatically called in the destructor.
	void EndTimer() {
		if (!is_active) {
			return;
		}
		is_active = false;
		profiler.End();
		query_metrics->UpdateMetric(metric_name, profiler.ElapsedNanos());
	}

	void Reset() {
		if (!is_active) {
			return;
		}
		profiler.Reset();
		is_active = false;
	}

private:
	optional_ptr<QueryMetrics> query_metrics;
	string metric_name;
	Profiler profiler;
	bool is_active;
};

} // namespace duckdb
